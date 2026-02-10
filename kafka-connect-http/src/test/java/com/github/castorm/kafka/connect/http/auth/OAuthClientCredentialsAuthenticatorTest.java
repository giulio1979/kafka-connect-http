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

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.kafka.connect.errors.RetriableException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OAuthClientCredentialsAuthenticatorTest {

    private MockWebServer mockWebServer;
    private OAuthClientCredentialsAuthenticator authenticator;

    @BeforeEach
    void setUp() throws IOException {
        mockWebServer = new MockWebServer();
        mockWebServer.start();
        authenticator = new OAuthClientCredentialsAuthenticator();
    }

    @AfterEach
    void tearDown() throws IOException {
        mockWebServer.shutdown();
    }

    @Test
    void whenValidResponse_thenReturnsBearerToken() throws InterruptedException {
        mockWebServer.enqueue(new MockResponse()
                .setBody("{\"access_token\":\"mytoken123\",\"expires_in\":3600,\"token_type\":\"Bearer\"}")
                .setResponseCode(200)
                .addHeader("Content-Type", "application/json"));

        authenticator.configure(configWith("my-client-id", "my-client-secret", "read"));

        assertThat(authenticator.getAuthorizationHeader())
                .isPresent()
                .hasValue("Bearer mytoken123");

        RecordedRequest request = mockWebServer.takeRequest();
        assertThat(request.getMethod()).isEqualTo("POST");
        assertThat(request.getHeader("Content-Type")).isEqualTo("application/x-www-form-urlencoded");
        assertThat(request.getHeader("Accept")).isEqualTo("application/json");

        String body = request.getBody().readUtf8();
        assertThat(body).contains("grant_type=client_credentials");
        assertThat(body).contains("client_id=my-client-id");
        assertThat(body).contains("client_secret=my-client-secret");
        assertThat(body).contains("client_secret=my-client-secret");
        assertThat(body).contains("scope=read");
    }

    @Test
    void whenNoScope_thenScopeOmittedFromBody() throws InterruptedException {
        mockWebServer.enqueue(new MockResponse()
                .setBody("{\"access_token\":\"token-no-scope\",\"expires_in\":3600}")
                .setResponseCode(200));

        authenticator.configure(configWith("cid", "csecret", ""));

        assertThat(authenticator.getAuthorizationHeader())
                .isPresent()
                .hasValue("Bearer token-no-scope");

        RecordedRequest request = mockWebServer.takeRequest();
        String body = request.getBody().readUtf8();
        assertThat(body).doesNotContain("scope=");
    }

    @Test
    void whenTokenCached_thenNoSecondRequest() {
        mockWebServer.enqueue(new MockResponse()
                .setBody("{\"access_token\":\"cached-token\",\"expires_in\":3600}")
                .setResponseCode(200));

        authenticator.configure(configWith("cid", "csecret", "read"));

        // First call fetches
        authenticator.getAuthorizationHeader();
        // Second call should use cache
        assertThat(authenticator.getAuthorizationHeader())
                .isPresent()
                .hasValue("Bearer cached-token");

        assertThat(mockWebServer.getRequestCount()).isEqualTo(1);
    }

    @Test
    void whenServerReturnsError_thenThrowsRetriableException() {
        mockWebServer.enqueue(new MockResponse()
                .setBody("{\"error\":\"invalid_client\"}")
                .setResponseCode(401));

        authenticator.configure(configWith("bad-id", "bad-secret", ""));

        assertThatThrownBy(() -> authenticator.getAuthorizationHeader())
                .isInstanceOf(RetriableException.class)
                .hasMessageContaining("OAuth token");
    }

    @Test
    void whenEmptyTokenInResponse_thenThrowsRetriableException() {
        mockWebServer.enqueue(new MockResponse()
                .setBody("{\"access_token\":\"\"}")
                .setResponseCode(200));

        authenticator.configure(configWith("cid", "csecret", ""));

        assertThatThrownBy(() -> authenticator.getAuthorizationHeader())
                .isInstanceOf(RetriableException.class);
    }

    @Test
    void whenCustomMethod_thenUsesConfiguredMethod() throws InterruptedException {
        mockWebServer.enqueue(new MockResponse()
                .setBody("{\"access_token\":\"put-token\",\"expires_in\":3600}")
                .setResponseCode(200));

        Map<String, String> cfg = configWith("cid", "csecret", "read");
        cfg.put("http.auth.oauth.method", "PUT");
        authenticator.configure(cfg);

        authenticator.getAuthorizationHeader();

        RecordedRequest request = mockWebServer.takeRequest();
        assertThat(request.getMethod()).isEqualTo("PUT");
    }

    @Test
    void whenCustomHeaders_thenUsesConfiguredHeaders() throws InterruptedException {
        mockWebServer.enqueue(new MockResponse()
                .setBody("{\"access_token\":\"custom-header-token\",\"expires_in\":3600}")
                .setResponseCode(200));

        Map<String, String> cfg = configWith("cid", "csecret", "read");
        cfg.put("http.auth.oauth.headers", "Content-Type=application/x-www-form-urlencoded,X-Custom=myvalue");
        authenticator.configure(cfg);

        authenticator.getAuthorizationHeader();

        RecordedRequest request = mockWebServer.takeRequest();
        assertThat(request.getHeader("X-Custom")).isEqualTo("myvalue");
    }

    @Test
    void whenCustomTokenKey_thenExtractsFromCustomKey() {
        mockWebServer.enqueue(new MockResponse()
                .setBody("{\"token\":\"custom-key-token\",\"expires_in\":3600}")
                .setResponseCode(200));

        Map<String, String> cfg = configWith("cid", "csecret", "");
        cfg.put("http.auth.oauth.token.key", "token");
        authenticator.configure(cfg);

        assertThat(authenticator.getAuthorizationHeader())
                .isPresent()
                .hasValue("Bearer custom-key-token");
    }

    @Test
    void whenSpecialCharsInCredentials_thenUrlEncoded() throws InterruptedException {
        mockWebServer.enqueue(new MockResponse()
                .setBody("{\"access_token\":\"encoded-token\",\"expires_in\":3600}")
                .setResponseCode(200));

        authenticator.configure(configWith("client&id=special", "secret with spaces", "scope:read"));

        authenticator.getAuthorizationHeader();

        RecordedRequest request = mockWebServer.takeRequest();
        String body = request.getBody().readUtf8();
        assertThat(body).contains("client_id=" + URLEncoder.encode("client&id=special", StandardCharsets.UTF_8));
        assertThat(body).contains("client_secret=" + URLEncoder.encode("secret with spaces", StandardCharsets.UTF_8));
        assertThat(body).contains("scope=" + URLEncoder.encode("scope:read", StandardCharsets.UTF_8));
    }

    private Map<String, String> configWith(String clientId, String clientSecret, String scope) {
        Map<String, String> config = new HashMap<>();
        config.put("http.auth.oauth.token.url", mockWebServer.url("/token").toString());
        config.put("http.auth.oauth.client.id", clientId);
        config.put("http.auth.oauth.client.secret", clientSecret);
        if (scope != null && !scope.isEmpty()) {
            config.put("http.auth.oauth.scope", scope);
        }
        return config;
    }
}
