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

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;

class OAuthClientCredentialsAuthenticatorConfigTest {

    @Test
    void whenNoConfig_thenDefaults() {
        OAuthClientCredentialsAuthenticatorConfig config = config(emptyMap());
        assertThat(config.getTokenUrl()).isEmpty();
        assertThat(config.getClientId()).isEmpty();
        assertThat(config.getClientSecret()).isEmpty();
        assertThat(config.getScope()).isEmpty();
        assertThat(config.getTokenExpirySeconds()).isEqualTo(60 * 59);
        assertThat(config.getMethod()).isEqualTo("POST");
        assertThat(config.getHeaders()).isEqualTo("Content-Type=application/x-www-form-urlencoded,Accept=application/json");
        assertThat(config.getTokenKey()).isEqualTo("access_token");
        assertThat(config.getGrantType()).isEqualTo("client_credentials");
    }

    @Test
    void whenTokenUrlConfigured_thenTokenUrl() {
        assertThat(config(Map.of("http.auth.oauth.token.url", "https://auth.example.com/token")).getTokenUrl())
                .isEqualTo("https://auth.example.com/token");
    }

    @Test
    void whenClientIdConfigured_thenClientId() {
        assertThat(config(Map.of("http.auth.oauth.client.id", "my-client")).getClientId())
                .isEqualTo("my-client");
    }

    @Test
    void whenClientSecretConfigured_thenClientSecret() {
        assertThat(config(Map.of("http.auth.oauth.client.secret", "my-secret")).getClientSecret())
                .isEqualTo("my-secret");
    }

    @Test
    void whenScopeConfigured_thenScope() {
        assertThat(config(Map.of("http.auth.oauth.scope", "read write")).getScope())
                .isEqualTo("read write");
    }

    @Test
    void whenTokenExpiryConfigured_thenTokenExpiry() {
        assertThat(config(Map.of("http.auth.oauth.token.expiry.seconds", "1800")).getTokenExpirySeconds())
                .isEqualTo(1800);
    }

    @Test
    void whenMethodConfigured_thenMethod() {
        assertThat(config(Map.of("http.auth.oauth.method", "PUT")).getMethod())
                .isEqualTo("PUT");
    }

    @Test
    void whenHeadersConfigured_thenHeaders() {
        assertThat(config(Map.of("http.auth.oauth.headers", "X-Custom=value")).getHeaders())
                .isEqualTo("X-Custom=value");
    }

    @Test
    void whenTokenKeyConfigured_thenTokenKey() {
        assertThat(config(Map.of("http.auth.oauth.token.key", "token")).getTokenKey())
                .isEqualTo("token");
    }

    @Test
    void whenGrantTypeConfigured_thenGrantType() {
        assertThat(config(Map.of("http.auth.oauth.grant.type", "authorization_code")).getGrantType())
                .isEqualTo("authorization_code");
    }

    private static OAuthClientCredentialsAuthenticatorConfig config(Map<String, String> config) {
        return new OAuthClientCredentialsAuthenticatorConfig(new HashMap<>(config));
    }
}
