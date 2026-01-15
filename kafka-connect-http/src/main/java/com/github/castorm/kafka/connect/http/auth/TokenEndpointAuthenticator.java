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
import okhttp3.logging.HttpLoggingInterceptor;
import org.apache.kafka.connect.errors.RetriableException;

import org.apache.kafka.connect.errors.ConnectException;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.github.castorm.kafka.connect.common.ConfigUtils.breakDownHeaders;
import static com.github.castorm.kafka.connect.common.ConfigUtils.breakDownMap;
import static okhttp3.logging.HttpLoggingInterceptor.Level.BODY;
import static okhttp3.logging.HttpLoggingInterceptor.Logger.DEFAULT;

public class TokenEndpointAuthenticator implements HttpAuthenticator {
    private final Function<Map<String, ?>, TokenEndpointAuthenticatorConfig> configFactory;
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
            cachedToken = null;
            try {
                cachedToken = fetchData();
                Instant jwtExpiry = getJwtExpiry(cachedToken);
                if (jwtExpiry != null) {
                    // Refresh 30 seconds before actual expiry to be safe
                    tokenExpiry = jwtExpiry.minusSeconds(30);
                } else {
                    tokenExpiry = Instant.now().plusSeconds(config.getTokenExpirySeconds());
                }
            } catch (Exception e) {
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
            String payload = new String(Base64.getUrlDecoder().decode(parts[1]));
            ObjectMapper mapper = new ObjectMapper();
            long exp = mapper.readTree(payload).path("exp").asLong();
            if (exp > 0) {
                return Instant.ofEpochSecond(exp);
            }
        } catch (Exception e) {
            // Token might not be a JWT or parsing failed, fallback to configured expiry
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
        OkHttpClient httpClient = new OkHttpClient();

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

            Response response = httpClient.newCall(builder.build()).execute();
                
            if (response.body() == null) return "";
            return response.body().string();
        } catch (IOException e) {
            throw new RetriableException("Error: " + e.getMessage(), e);
        } catch (IllegalArgumentException e) {
            throw new ConnectException("Error: " + e.getMessage(), e);
        }
    }
}