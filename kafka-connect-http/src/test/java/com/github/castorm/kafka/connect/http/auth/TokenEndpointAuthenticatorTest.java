package com.github.castorm.kafka.connect.http.auth;

import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.*;
class TokenEndpointAuthenticatorTest {

    @Test
    void getAuthorizationHeader() {
        HashMap orig = new HashMap();
        orig.put("http.auth.url","https://www.google.com");
        orig.put("http.auth.body","refresh_token=mytest&grant_type=refresh_token");
        orig.put("http.auth.tokenkeypath","access_token");
        orig.put("http.auth.method","POST");
        orig.put("http.token.request.headers","Content-Type=application/x-www-form-urlencoded,Authorization=Basic Mytest==");

        TokenEndpointAuthenticator t = new TokenEndpointAuthenticator();
        t.configure(orig);

        //System.out.println(t.getAuthorizationHeader());

    }
}