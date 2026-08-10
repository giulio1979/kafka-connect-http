package com.github.castorm.kafka.connect.http.auth;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class TokenEndpointAuthenticatorTest {

    private MockWebServer mockWebServer;
    private TokenEndpointAuthenticator authenticator;

    @BeforeEach
    void setUp() throws IOException {
        mockWebServer = new MockWebServer();
        mockWebServer.start();
        authenticator = new TokenEndpointAuthenticator();
    }

    @AfterEach
    void tearDown() throws IOException {
        mockWebServer.shutdown();
    }

    @Test
    void whenInvalidated_thenFetchesNewTokenUsingConfiguredContentType() throws InterruptedException {
        mockWebServer.enqueue(tokenResponse("first-token"));
        mockWebServer.enqueue(tokenResponse("second-token"));
        authenticator.configure(config());

        assertThat(authenticator.getAuthorizationHeader()).hasValue("Bearer first-token");
        assertThat(authenticator.getAuthorizationHeader()).hasValue("Bearer first-token");
        assertThat(authenticator.getGeneration()).isEqualTo(1);

        authenticator.invalidate();

        assertThat(authenticator.getAuthorizationHeader()).hasValue("Bearer second-token");
        assertThat(authenticator.getGeneration()).isEqualTo(2);
        assertThat(mockWebServer.getRequestCount()).isEqualTo(2);

        RecordedRequest request = mockWebServer.takeRequest();
        assertThat(request.getHeader("Content-Type")).isEqualTo("application/x-www-form-urlencoded");
        assertThat(request.getBody().readUtf8()).isEqualTo("refresh_token=mytest&grant_type=refresh_token");
    }

    private MockResponse tokenResponse(String token) {
        return new MockResponse()
                .setResponseCode(200)
                .setBody("{\"access_token\":\"" + token + "\",\"expires_in\":3600}");
    }

    private Map<String, String> config() {
        Map<String, String> config = new HashMap<>();
        config.put("http.auth.url", mockWebServer.url("/token").toString());
        config.put("http.auth.body", "refresh_token=mytest&grant_type=refresh_token");
        config.put("http.auth.tokenkeypath", "access_token");
        config.put("http.auth.method", "POST");
        config.put("http.token.request.headers", "Content-Type=application/x-www-form-urlencoded");
        return config;
    }
}