package com.github.castorm.kafka.connect.http;

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

import com.github.castorm.kafka.connect.http.ack.ConfirmationWindow;
import com.github.castorm.kafka.connect.http.client.spi.HttpClient;
import com.github.castorm.kafka.connect.http.model.HttpRequest;
import com.github.castorm.kafka.connect.http.model.HttpResponse;
import com.github.castorm.kafka.connect.http.model.Offset;
import com.github.castorm.kafka.connect.http.record.spi.SourceRecordFilterFactory;
import com.github.castorm.kafka.connect.http.record.spi.SourceRecordSorter;
import com.github.castorm.kafka.connect.http.request.spi.HttpRequestFactory;
import com.github.castorm.kafka.connect.http.response.spi.HttpResponseParser;

import com.github.castorm.kafka.connect.timer.TimerThrottler;
import edu.emory.mathcs.backport.java.util.Collections;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.github.castorm.kafka.connect.common.VersionUtils.getVersion;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;

@Slf4j
public class HttpSourceTask extends SourceTask {

    public static final String AUTOTIMESTAMP = "AUTOTIMESTAMP";
    private final Function<Map<String, String>, HttpSourceConnectorConfig> configFactory;

    private TimerThrottler throttler;

    private HttpRequestFactory requestFactory;

    private HttpClient requestExecutor;

    private HttpResponseParser responseParser;

    private SourceRecordSorter recordSorter;

    private SourceRecordFilterFactory recordFilterFactory;

    private ConfirmationWindow<Map<String, ?>> confirmationWindow = new ConfirmationWindow<>(emptyList());

    private String nextPageOffsetField;
    private String hasNextPageField;

    private String autoDateInitialOffset;
    private String sautoDateIncrement;
    private String sautoDateBackoff;

    @Getter
    private Offset offset;

    HttpSourceTask(Function<Map<String, String>, HttpSourceConnectorConfig> configFactory) {
        this.configFactory = configFactory;
    }

    public HttpSourceTask() {
        this(HttpSourceConnectorConfig::new);
    }

    @Override
    public void start(Map<String, String> settings) {

        HttpSourceConnectorConfig config = configFactory.apply(settings);

        throttler = config.getThrottler();
        requestFactory = config.getRequestFactory();
        requestExecutor = config.getClient();
        responseParser = config.getResponseParser();
        recordSorter = config.getRecordSorter();
        recordFilterFactory = config.getRecordFilterFactory();
        offset = loadOffset(config.getInitialOffset());
        nextPageOffsetField = config.getNextPageOffsetField();
        hasNextPageField  = config.getHasNextPageField();

        autoDateInitialOffset = config.getAutoDateInitialOffset();
        sautoDateIncrement = config.getAutoDateIncrement();
        sautoDateBackoff = config.getAutoDateBackoff();
    }

    private Offset loadOffset(Map<String, String> initialOffset) {
        Map<String, Object> restoredOffset = ofNullable(context.offsetStorageReader().offset(emptyMap())).orElseGet(Collections::emptyMap);
        return Offset.of(!restoredOffset.isEmpty() ? restoredOffset : initialOffset);
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {

        throttler.throttle(offset.getTimestamp().orElseGet(Instant::now));
        List<SourceRecord> allRecords = new ArrayList<>();
        DateTimeFormatter formatter = DateTimeFormatter.ISO_DATE_TIME;
        long autoOffset = 0;
        long autoDateIncrement = 0;
        long autoDateBackoff = 0;
        boolean pagingEnabled = nextPageOffsetField != null && !nextPageOffsetField.isEmpty() && hasNextPageField != null && !hasNextPageField.isEmpty();

        if( autoDateInitialOffset != null && !autoDateInitialOffset.isEmpty()) {
            try {
                LocalDateTime dateTime = LocalDateTime.parse(autoDateInitialOffset, formatter);
                Instant timestamp = dateTime.atZone(ZoneId.of("UTC")).toInstant();
                autoDateIncrement = Long.parseLong(sautoDateIncrement);
                autoDateBackoff = Long.parseLong(sautoDateBackoff);
                autoOffset = timestamp.toEpochMilli();
            } catch (Exception e) {
                e.printStackTrace();
            }

            if (offset.toMap().containsKey(AUTOTIMESTAMP)) {
                autoOffset = (Long) offset.toMap().get(AUTOTIMESTAMP);
            }

            offset.setValue(AUTOTIMESTAMP, autoOffset);
        }

        String hasNextPageFlag = "true";
        String nextPageValue = "";

        if(pagingEnabled){
            offset.setValue(nextPageOffsetField, "");
            offset.setValue(hasNextPageField, "");
        }

        while(hasNextPageFlag.matches("true")) {
            HttpRequest request = requestFactory.createRequest(offset);

            log.info("Request for offset {}", offset.toString());
            log.info("Request for page {}", request.toString());
            log.info("Request for initial page {} hasNextPageFlag {}", nextPageValue, hasNextPageFlag);

            HttpResponse response = execute(request);

            List<SourceRecord> records = responseParser.parse(response);

            if(!records.isEmpty()) {
                allRecords.addAll(records);
                if(pagingEnabled){
                    nextPageValue = (String) records.get(0).sourceOffset().get(nextPageOffsetField);
                    hasNextPageFlag = (String) records.get(0).sourceOffset().get(hasNextPageField);                
                    offset.setValue(nextPageOffsetField, nextPageValue);
                    offset.setValue(hasNextPageField, hasNextPageFlag);
                } else {
                    hasNextPageFlag = "";
                }
            } else {
                hasNextPageFlag = "";
            }
            Thread.sleep(300);
        }

        List<SourceRecord> unseenRecords = recordSorter.sort(allRecords).stream()
                .filter(recordFilterFactory.create(offset))
                .collect(toList());

        if(autoDateInitialOffset != null && !autoDateInitialOffset.isEmpty()) {
            autoOffset = autoOffset + autoDateIncrement - autoDateBackoff;
            if (autoOffset > new Date().getTime()) {
                autoOffset = new Date().getTime() - autoDateBackoff;
            }
            for(SourceRecord s: allRecords) {
                ((Map<String,Long>)s.sourceOffset()).put(AUTOTIMESTAMP, Long.valueOf(autoOffset));
            }
            offset.setValue(AUTOTIMESTAMP, autoOffset);
            log.info("AutoOffset Patch {}", offset.toString());
        }

        log.info("Request for offset {} yields {}/{} new records", offset.toMap(), unseenRecords.size(), allRecords.size());

        confirmationWindow = new ConfirmationWindow<>(extractOffsets(unseenRecords));

        return unseenRecords;
    }

    private HttpResponse execute(HttpRequest request) {
        try {
            return requestExecutor.execute(request);
        } catch (IOException e) {
            throw new RetriableException(e);
        }
    }

    private static List<Map<String, ?>> extractOffsets(List<SourceRecord> recordsToSend) {
        return recordsToSend.stream()
                .map(SourceRecord::sourceOffset)
                .collect(toList());
    }

    @Override
    public void commitRecord(SourceRecord record, RecordMetadata metadata) {
        confirmationWindow.confirm(record.sourceOffset());
    }

    @Override
    public void commit() {
        offset = confirmationWindow.getLowWatermarkOffset()
                .map(Offset::of)
                .orElse(offset);

        log.debug("Offset set to {}", offset);
    }

    @Override
    public void stop() {
        // Nothing to do, no resources to release
    }

    @Override
    public String version() {
        return getVersion();
    }
}
