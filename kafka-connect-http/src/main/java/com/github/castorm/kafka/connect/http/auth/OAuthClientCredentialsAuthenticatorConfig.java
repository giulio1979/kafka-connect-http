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

import lombok.Getter;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.PASSWORD;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

@Getter
public class OAuthClientCredentialsAuthenticatorConfig extends AbstractConfig {

    private static final String TOKEN_URL = "http.auth.oauth.token.url";
    private static final String CLIENT_ID = "http.auth.oauth.client.id";
    private static final String CLIENT_SECRET = "http.auth.oauth.client.secret";
    private static final String SCOPE = "http.auth.oauth.scope";
    private static final String TOKEN_EXPIRY = "http.auth.oauth.token.expiry.seconds";
    private static final String METHOD = "http.auth.oauth.method";
    private static final String HEADERS = "http.auth.oauth.headers";
    private static final String TOKEN_KEY = "http.auth.oauth.token.key";
    private static final String GRANT_TYPE = "http.auth.oauth.grant.type";

    private final String tokenUrl;
    private final String clientId;
    private final String clientSecret;
    private final String scope;
    private final Integer tokenExpirySeconds;
    private final String method;
    private final String headers;
    private final String tokenKey;
    private final String grantType;

    public OAuthClientCredentialsAuthenticatorConfig(Map<String, ?> originals) {
        super(config(), originals);
        tokenUrl = getString(TOKEN_URL);
        clientId = getString(CLIENT_ID);
        clientSecret = getPassword(CLIENT_SECRET).value();
        scope = getString(SCOPE);
        tokenExpirySeconds = getInt(TOKEN_EXPIRY);
        method = getString(METHOD);
        headers = getString(HEADERS);
        tokenKey = getString(TOKEN_KEY);
        grantType = getString(GRANT_TYPE);
    }

    public static ConfigDef config() {
        return new ConfigDef()
                .define(TOKEN_URL, STRING, "", HIGH, "OAuth2 token endpoint URL")
                .define(CLIENT_ID, STRING, "", HIGH, "OAuth2 client ID")
                .define(CLIENT_SECRET, PASSWORD, "", HIGH, "OAuth2 client secret")
                .define(SCOPE, STRING, "", MEDIUM, "OAuth2 scopes (space-separated)")
                .define(TOKEN_EXPIRY, INT, 60 * 59, MEDIUM, "Fallback token expiry in seconds if not provided in response")
                .define(METHOD, STRING, "POST", MEDIUM, "HTTP method for the token request")
                .define(HEADERS, STRING, "Content-Type=application/x-www-form-urlencoded,Accept=application/json", MEDIUM,
                        "HTTP headers for the token request as comma-separated Key=Value pairs")
                .define(TOKEN_KEY, STRING, "access_token", MEDIUM, "JSON key to extract the access token from the response")
                .define(GRANT_TYPE, STRING, "client_credentials", MEDIUM, "OAuth2 grant type");
    }
}
