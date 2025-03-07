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

import com.github.castorm.kafka.connect.http.auth.spi.HttpAuthenticator;
import okhttp3.Credentials;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static org.apache.commons.lang.StringUtils.isEmpty;

public class BearerAuthenticator implements HttpAuthenticator {

    private final Function<Map<String, ?>, BearerAuthenticatorConfig> configFactory;

    Optional<String> header;

    public BearerAuthenticator() {
        this(BearerAuthenticatorConfig::new);
    }

    public BearerAuthenticator(Function<Map<String, ?>, BearerAuthenticatorConfig> configFactory) {
        this.configFactory = configFactory;
    }

    @Override
    public void configure(Map<String, ?> configs) {

        BearerAuthenticatorConfig config = configFactory.apply(configs);

        if (!isEmpty(config.getBearer()) ) {
            header = Optional.of(config.getBearer());
        } else {
            header = Optional.empty();
        }
    }

    @Override
    public Optional<String> getAuthorizationHeader() {
        return header;
    }
}
