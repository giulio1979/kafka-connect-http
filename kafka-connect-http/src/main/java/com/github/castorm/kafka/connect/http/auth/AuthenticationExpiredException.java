package com.github.castorm.kafka.connect.http.auth;

import org.apache.kafka.connect.errors.RetriableException;

public class AuthenticationExpiredException extends RetriableException {

    public AuthenticationExpiredException(String message) {
        super(message);
    }
}