package com.github.castorm.kafka.connect.http.response;

/*-
 * #%L
 * Kafka Connect HTTP Plugin
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

import com.github.castorm.kafka.connect.http.model.HttpRecord;
import com.github.castorm.kafka.connect.http.model.HttpResponse;
import com.github.castorm.kafka.connect.http.response.spi.HttpResponseParser;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.util.Collections.emptyList;

@Slf4j
@RequiredArgsConstructor
public class StatusCodeFilterResponseParser implements HttpResponseParser {

    private final Function<Map<String, ?> , StatusCodeFilterResponseParserConfig> configFactory;

    private HttpResponseParser delegate;

    public StatusCodeFilterResponseParser() {
        this(StatusCodeFilterResponseParserConfig::new);
    }

    @Override
    public void configure(Map<String, ?> settings) {
        delegate = configFactory.apply(settings).getDelegateParser();
    }

    @Override
    public List<HttpRecord> parse(HttpResponse response) {
        if (response.getCode() >= 200 && response.getCode() < 300) {
            return delegate.parse(response);
        } else if (response.getCode() >= 300 && response.getCode() < 400) {
            log.warn("Unexpected HttpResponse status code: {}, continuing with no records", response.getCode());
            return emptyList();
        } else {
            log.error("Unexpected HttpResponse status code: {}, halting the connector. Body: {}", response.getCode(), response.getBody());
            throw new IllegalStateException(String.format("Unexpected HttpResponse status code: %s", response.getCode()));
        }
    }
}
