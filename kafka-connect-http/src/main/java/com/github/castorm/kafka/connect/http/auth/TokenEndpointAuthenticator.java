package com.github.castorm.kafka.connect.http.auth;

/*-
 * #%L
 * Kafka Connect HTTP
 * %%
 * Copyright (C) 2020 - 2021 Cástor Rodríguez
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.castorm.kafka.connect.http.auth.spi.HttpAuthenticator;

import java.util.Base64;
import okhttp3.*;
import org.apache.kafka.connect.errors.RetriableException;

import org.apache.kafka.connect.errors.ConnectException;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.castorm.kafka.connect.common.ConfigUtils.breakDownMap;

public class TokenEndpointAuthenticator implements HttpAuthenticator {
    private static final Logger log = LoggerFactory.getLogger(TokenEndpointAuthenticator.class);
    private static final int TOKEN_EXPIRY_BUFFER_SECONDS = 60;

    private final Function<Map<String, ?>, TokenEndpointAuthenticatorConfig> configFactory;
    private final OkHttpClient httpClient = new OkHttpClient();
    private TokenEndpointAuthenticatorConfig config;
    private String cachedToken = null;
    private Instant tokenExpiry = Instant.EPOCH;

    public TokenEndpointAuthenticator() {
        this(TokenEndpointAuthenticatorConfig::new);
    }

    public TokenEndpointAuthenticator(Function<Map<String, ?>, TokenEndpointAuthenticatorConfig> configFactory) {
        this.configFactory = configFactory;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        this.config = configFactory.apply(configs);
    }


    @Override
    public Optional<String> getAuthorizationHeader() {
        if (isTokenExpired()) {
            log.info("Token expired or absent, refreshing. Previous expiry: {}", tokenExpiry);
            cachedToken = null;
            tokenExpiry = Instant.EPOCH;
            try {
                cachedToken = fetchData();
                Instant jwtExpiry = getJwtExpiry(cachedToken);
                if (jwtExpiry != null) {
                    tokenExpiry = jwtExpiry.minusSeconds(TOKEN_EXPIRY_BUFFER_SECONDS);
                    log.info("Token refreshed. JWT exp: {}, will refresh at: {}", jwtExpiry, tokenExpiry);
                } else {
                    tokenExpiry = Instant.now().plusSeconds(config.getTokenExpirySeconds());
                    log.info("Token refreshed (non-JWT). Will refresh at: {} (configured expiry: {}s)",
                            tokenExpiry, config.getTokenExpirySeconds());
                }
            } catch (Exception e) {
                log.error("Failed to refresh token", e);
                throw new RetriableException("Error: " + e.getMessage(), e);
            }
            if (cachedToken == null || cachedToken.isEmpty()) {
                throw new RetriableException("Error: Access token is empty.");
            }
        }
        return Optional.of("Bearer " + cachedToken);
    }

    private Instant getJwtExpiry(String token) {
        try {
            String[] parts = token.split("\\.");
            if (parts.length < 2) return null;
            // JWT Base64URL may lack padding — add it before decoding
            String base64 = parts[1];
            int padding = (4 - base64.length() % 4) % 4;
            base64 = base64 + "=".repeat(padding);
            String payload = new String(Base64.getUrlDecoder().decode(base64));
            ObjectMapper mapper = new ObjectMapper();
            long exp = mapper.readTree(payload).path("exp").asLong();
            if (exp > 0) {
                return Instant.ofEpochSecond(exp);
            }
        } catch (Exception e) {
            log.warn("Could not parse JWT expiry, falling back to configured expiry", e);
        }
        return null;
    }

    public String fetchData() {
        String data = execute(config.getAuthUrl(), config.getAuthMethod(), config.getHeaders(), config.getAuthBody().value());
        String key = config.getAuthChainUrl() != null && !config.getAuthChainUrl().isEmpty() ?
                config.getAuthChainTokenKey() : config.getTokenKeyPath();
        String token = parseToken(data, key);

        if (config.getAuthChainUrl() != null && !config.getAuthChainUrl().isEmpty()) {
            String chainHeaders = config.getAuthChainHeaders().replace("{{token}}", token);
            String chainData = execute(config.getAuthChainUrl(), config.getAuthChainMethod(), chainHeaders, config.getAuthChainBody().value());
            token = parseToken(chainData, config.getTokenKeyPath());
        }

        return token;
    }

    private String parseToken(String response, String key) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            String token = objectMapper.readTree(response).path(key).asText();
            if (token == null || token.isBlank()) {
                throw new RetriableException("Error: No token found at " + key + " Response was: " + response);
            }
            return token;
        } catch (JsonProcessingException e) {
            throw new RetriableException("Error: " + e.getMessage(), e);
        }
    }

    private boolean isTokenExpired() {
        return Instant.now().isAfter(tokenExpiry) || cachedToken == null || cachedToken.isEmpty();
    }

    private String execute(String url, String method, String headersStr, String bodyStr) {
        try {
            Map<String, String> m = breakDownMap(headersStr);
            okhttp3.Headers headers = okhttp3.Headers.of(m);

            Request.Builder builder = new Request.Builder()
                    .url(url)
                    .headers(headers);

            if (method.equalsIgnoreCase("POST")) {
                RequestBody body = RequestBody.create(MediaType.parse("application/json"), bodyStr.getBytes());
                builder.post(body);
            } else if (method.equalsIgnoreCase("PUT")) {
                RequestBody body = RequestBody.create(MediaType.parse("application/json"), bodyStr.getBytes());
                builder.put(body);
            } else {
                builder.get();
            }

            try (Response response = httpClient.newCall(builder.build()).execute()) {
                String responseBody = response.body() != null ? response.body().string() : "";

                if (!response.isSuccessful()) {
                    log.error("Token endpoint {} returned HTTP {}: {}", url, response.code(), responseBody);
                    throw new RetriableException(
                            "Token endpoint returned HTTP " + response.code() + ": " + responseBody);
                }

                return responseBody;
            }
        } catch (IOException e) {
            throw new RetriableException("Error: " + e.getMessage(), e);
        } catch (IllegalArgumentException e) {
            throw new ConnectException("Error: " + e.getMessage(), e);
        }
    }
}