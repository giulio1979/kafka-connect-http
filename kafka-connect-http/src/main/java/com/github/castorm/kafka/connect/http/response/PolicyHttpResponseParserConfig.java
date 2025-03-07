package com.github.castorm.kafka.connect.http.response;

/*-
 * #%L
 * kafka-connect-http
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

import com.github.castorm.kafka.connect.http.response.spi.HttpResponseParser;
import com.github.castorm.kafka.connect.http.response.spi.HttpResponsePolicy;
import lombok.Getter;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

import static com.github.castorm.kafka.connect.common.ConfigUtils.breakDownMap;
import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Type.CLASS;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

@Getter
public class PolicyHttpResponseParserConfig extends AbstractConfig {

    private static final String PARSER_DELEGATE = "http.response.policy.parser";
    private static final String POLICY = "http.response.policy";

    private final HttpResponseParser delegateParser;

    private final HttpResponsePolicy policy;

    public PolicyHttpResponseParserConfig(Map<String, ?> originals) {
        super(config(), originals);
        delegateParser = getConfiguredInstance(PARSER_DELEGATE, HttpResponseParser.class);
        policy = getConfiguredInstance(POLICY, HttpResponsePolicy.class);
    }

    public static ConfigDef config() {
        return new ConfigDef()
                .define(PARSER_DELEGATE, CLASS, KvHttpResponseParser.class, HIGH, "Response Parser Delegate Class")
                .define(POLICY, CLASS, StatusCodeHttpResponsePolicy.class, HIGH, "Response Policy Class");
    }
}
