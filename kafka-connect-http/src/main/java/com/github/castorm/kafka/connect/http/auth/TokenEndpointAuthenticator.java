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
                tokenExpiry = Instant.now().plusSeconds(config.getTokenExpirySeconds());
            } catch (Exception e) {
                throw new RetriableException("Error: " + e.getMessage(), e);
            }
            if (cachedToken == null || cachedToken.isEmpty()) {
                throw new RetriableException("Error: Access token is empty.");
            }
        }
        return Optional.of("Bearer " + cachedToken);
    }

    public String fetchData() {
        String credentialsBody = config.getAuthBody().value();
        RequestBody requestBody = FormBody.create(credentialsBody.getBytes());

        String response = execute(requestBody);

        ObjectMapper objectMapper = new ObjectMapper();

        String accessToken;
        try {
            accessToken = objectMapper.readTree(response).path(config.getTokenKeyPath()).asText();
        } catch (JsonProcessingException e) {
            throw new RetriableException("Error: " + e.getMessage(), e);
        }

        if (accessToken.isBlank()) {
            throw new RetriableException("Error: No access token found at " + config.getTokenKeyPath() + " Response was:" + response);
        }

        return accessToken;
    }

    private boolean isTokenExpired() {
        return Instant.now().isAfter(tokenExpiry) || cachedToken == null || cachedToken.isEmpty();
    }

    private String execute(RequestBody requestBody) {
        OkHttpClient httpClient = new OkHttpClient();

        try {
            Map<String, String> m = breakDownMap(config.getHeaders());
            okhttp3.Headers headers = okhttp3.Headers.of(m);

            Request request = null;
            if(config.getAuthMethod().toString().equalsIgnoreCase("POST"))
                request = new Request.Builder()
                        .url(config.getAuthUrl())
                        .headers(headers)
                        .post(requestBody).build();
            else
                request = new Request.Builder()
                        .url(config.getAuthUrl())
                        .headers(headers)
                        .get().build();

            Response response = httpClient.newCall(request).execute();

            return response.body().string();
        } catch (IOException e) {
            throw new RetriableException("Error: " + e.getMessage(), e);
        } catch (IllegalArgumentException e) {
            throw new ConnectException("Error: " + e.getMessage(), e);
        }
    }
}