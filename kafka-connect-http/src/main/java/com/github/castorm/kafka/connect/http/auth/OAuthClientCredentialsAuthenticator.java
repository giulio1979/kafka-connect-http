package com.github.castorm.kafka.connect.http.auth;

/*-
 * #%L
 * Kafka Connect HTTP
 * %%
 * Copyright (C) 2020 CastorM
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.castorm.kafka.connect.http.auth.spi.HttpAuthenticator;
import okhttp3.*;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.github.castorm.kafka.connect.common.ConfigUtils.breakDownMap;

public class OAuthClientCredentialsAuthenticator implements HttpAuthenticator {

    private final Function<Map<String, ?>, OAuthClientCredentialsAuthenticatorConfig> configFactory;
    private OAuthClientCredentialsAuthenticatorConfig config;
    private String cachedToken = null;
    private Instant tokenExpiry = Instant.EPOCH;

    public OAuthClientCredentialsAuthenticator() {
        this(OAuthClientCredentialsAuthenticatorConfig::new);
    }

    public OAuthClientCredentialsAuthenticator(Function<Map<String, ?>, OAuthClientCredentialsAuthenticatorConfig> configFactory) {
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
                cachedToken = fetchToken();
                Instant jwtExpiry = getJwtExpiry(cachedToken);
                if (jwtExpiry != null) {
                    // Refresh 30 seconds before actual expiry to be safe
                    tokenExpiry = jwtExpiry.minusSeconds(30);
                } else {
                    tokenExpiry = Instant.now().plusSeconds(config.getTokenExpirySeconds());
                }
            } catch (Exception e) {
                throw new RetriableException("OAuth token fetch failed: " + e.getMessage(), e);
            }
            if (cachedToken == null || cachedToken.isEmpty()) {
                throw new RetriableException("OAuth token fetch returned empty access token");
            }
        }
        return Optional.of("Bearer " + cachedToken);
    }

    String fetchToken() {
        String responseBody = executeTokenRequest();
        return parseToken(responseBody);
    }

    private String executeTokenRequest() {
        OkHttpClient httpClient = new OkHttpClient();

        try {
            Map<String, String> headerMap = breakDownMap(config.getHeaders());
            Headers headers = Headers.of(headerMap);

            String body = buildFormBody();

            Request.Builder builder = new Request.Builder()
                    .url(config.getTokenUrl())
                    .headers(headers);

            String method = config.getMethod().toUpperCase();
            if ("POST".equals(method)) {
                builder.post(RequestBody.create(MediaType.parse("application/x-www-form-urlencoded"), body.getBytes(StandardCharsets.UTF_8)));
            } else if ("PUT".equals(method)) {
                builder.put(RequestBody.create(MediaType.parse("application/x-www-form-urlencoded"), body.getBytes(StandardCharsets.UTF_8)));
            } else {
                builder.get();
            }

            Response response = httpClient.newCall(builder.build()).execute();

            if (!response.isSuccessful()) {
                String errorBody = response.body() != null ? response.body().string() : "";
                throw new RetriableException("OAuth token request failed with HTTP " + response.code() + ": " + errorBody);
            }

            if (response.body() == null) {
                throw new RetriableException("OAuth token request returned empty response");
            }
            return response.body().string();
        } catch (IOException e) {
            throw new RetriableException("OAuth token request IO error: " + e.getMessage(), e);
        } catch (IllegalArgumentException e) {
            throw new ConnectException("OAuth token request configuration error: " + e.getMessage(), e);
        }
    }

    private String buildFormBody() {
        StringBuilder sb = new StringBuilder();
        sb.append("grant_type=").append(urlEncode(config.getGrantType()));
        sb.append("&client_id=").append(urlEncode(config.getClientId()));
        sb.append("&client_secret=").append(urlEncode(config.getClientSecret()));
        if (config.getScope() != null && !config.getScope().isEmpty()) {
            sb.append("&scope=").append(urlEncode(config.getScope()));
        }
        return sb.toString();
    }

    private String parseToken(String response) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            String tokenKey = config.getTokenKey();
            String token = objectMapper.readTree(response).path(tokenKey).asText();
            if (token == null || token.isBlank()) {
                throw new RetriableException("No token found at key '" + tokenKey + "' in response: " + response);
            }

            // Attempt to read expires_in from response for smarter caching
            long expiresIn = objectMapper.readTree(response).path("expires_in").asLong(0);
            if (expiresIn > 0) {
                // Use response-provided expiry with 30s buffer, unless JWT expiry will override
                tokenExpiry = Instant.now().plusSeconds(expiresIn).minusSeconds(30);
            }

            return token;
        } catch (IOException e) {
            throw new RetriableException("Failed to parse OAuth token response: " + e.getMessage(), e);
        }
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
            // Token might not be a JWT or parsing failed, fallback to configured/response expiry
        }
        return null;
    }

    private boolean isTokenExpired() {
        return Instant.now().isAfter(tokenExpiry) || cachedToken == null || cachedToken.isEmpty();
    }

    private static String urlEncode(String value) {
        try {
            return URLEncoder.encode(value, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            // UTF-8 is always supported
            throw new ConnectException("Failed to URL-encode value", e);
        }
    }
}
