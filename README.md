# Kafka Connect HTTP Connector
[![Build](https://github.com/castorm/kafka-connect-http/workflows/Build/badge.svg)](https://github.com/castorm/kafka-connect-http/actions?query=workflow%3ABuild)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/1c507b00f24f4a979d6c65dd0b5fcdb6)](https://www.codacy.com/manual/castorm/kafka-connect-http?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=castorm/kafka-connect-http&amp;utm_campaign=Badge_Grade)
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fcastorm%2Fkafka-connect-http.svg?type=shield)](https://app.fossa.com/projects/git%2Bgithub.com%2Fcastorm%2Fkafka-connect-http?ref=badge_shield)
[![Release to GitHub](https://github.com/castorm/kafka-connect-http/workflows/Release%20to%20GitHub/badge.svg)](https://github.com/castorm/kafka-connect-http/actions?query=workflow%3A%22Release+to+GitHub%22)
[![Release to Maven Central](https://github.com/castorm/kafka-connect-http/workflows/Release%20to%20Maven%20Central/badge.svg)](https://github.com/castorm/kafka-connect-http/actions?query=workflow%3A%22Release+to+Maven+Central%22)
[![Maven Central](https://img.shields.io/maven-central/v/com.github.castorm/kafka-connect-http.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22com.github.castorm%22%20AND%20a:%22kafka-connect-http%22)

Kafka Connect connector that enables [Change Data Capture](docs/Change_Data_Capture.md) from JSON/HTTP APIs into Kafka.

## This connector is for you if
*   You want to (live) replicate a dataset exposed through JSON/HTTP API
*   You want to do so efficiently
*   You want to capture only changes, not full snapshots
*   You want to do so via configuration, with no custom coding
*   You want to be able to extend the connector if it comes to that

### Examples

See [examples](examples), e.g. 
*   [Jira Issues Search API](examples/jira-issues-search.md)
*   [Elasticsearch Search API](examples/elasticsearch-search.md)

## Getting Started

If your Kafka Connect deployment is automated and packaged with Maven, you can unpack the artifact on Kafka Connect
plugins folder.
```xml
<plugin>
    <artifactId>maven-dependency-plugin</artifactId>
    <execution>
        <id>copy-kafka-connect-plugins</id>
        <phase>prepare-package</phase>
        <goals>
            <goal>unpack</goal>
        </goals>
        <configuration>
            <outputDirectory>${project.build.directory}/docker-build/plugins</outputDirectory>
            <artifactItems>
                <artifactItem>
                    <groupId>com.github.castorm</groupId>
                    <artifactId>kafka-connect-http</artifactId>
                    <version>0.8.11</version>
                    <type>tar.gz</type>
                    <classifier>plugin</classifier>
                </artifactItem>
            </artifactItems>
        </configuration>
    </execution>
</plugin>
```
Otherwise, you'll have to do it manually by downloading the package from the
[Releases Page](https://github.com/castorm/kafka-connect-http/releases).

More details on how to [Install Connectors](https://docs.confluent.io/current/connect/managing/install.html).

## Source Connector
`com.github.castorm.kafka.connect.http.HttpSourceConnector`

### Extension points
The connector can be easily extended by implementing your own version of any of the components below.

These are better understood by looking at the source task implementation:
```java
public List<SourceRecord> poll() throws InterruptedException {

    throttler.throttle(offset.getTimestamp().orElseGet(Instant::now));

    HttpRequest request = requestFactory.createRequest(offset);

    HttpResponse response = requestExecutor.execute(request);

    List<SourceRecord> records = responseParser.parse(response);

    List<SourceRecord> unseenRecords = recordSorter.sort(records).stream()
            .filter(recordFilterFactory.create(offset))
            .collect(toList());

    confirmationWindow = new ConfirmationWindow<>(extractOffsets(unseenRecords));

    return unseenRecords;
}

public void commitRecord(SourceRecord record, RecordMetadata metadata) {
    confirmationWindow.confirm(record.sourceOffset());
}

public void commit() {
    offset = confirmationWindow.getLowWatermarkOffset()
            .map(Offset::of)
            .orElse(offset);
}
```

---
<a name="timer"/>

### `Timer`: Throttling `HttpRequest`
Controls the rate at which HTTP requests are performed by informing the task, how long until the next execution is due.

> #### `http.timer`
> ```java
> public interface Timer extends Configurable {
> 
>     Long getRemainingMillis();
> 
>     default void reset(Instant lastZero) {
>         // Do nothing
>     }
> }
> ```
> *   Type: `Class`
> *   Default: `com.github.castorm.kafka.connect.timer.AdaptableIntervalTimer`
> *   Available implementations:
>     *   `com.github.castorm.kafka.connect.timer.FixedIntervalTimer`
>     *   `com.github.castorm.kafka.connect.timer.AdaptableIntervalTimer`

#### Throttling `HttpRequest` with `FixedIntervalThrottler`

Throttles rate of requests based on a fixed interval. 

> ##### `http.timer.interval.millis`
> Interval in between requests
> *   Type: `Long`
> *   Default: `60000`

#### Throttling HttpRequests with `AdaptableIntervalThrottler`

Throttles rate of requests based on a fixed interval.
It has, however, two modes of operation, with two different  intervals:
*   **Up to date** No new records in last poll, or there were new records, but "recently" created (shorter than interval) 
*   **Catching up** There were new records in last poll, but they were created "long ago" (longer than interval)

> ##### `http.timer.interval.millis`
> Interval in between requests when up-to-date
> 
> *   Type: `Long`
> *   Default: `60000`
> 
> ##### `http.timer.catchup.interval.millis`
> Interval in between requests when catching up
> *   Type: `Long`
> *   Default: `30000`


---
<a name="request"/>

### `HttpRequestFactory`: Creating a `HttpRequest`
The first thing our connector will need to do is creating a `HttpRequest`.

> #### `http.request.factory`
> ```java
> public interface HttpRequestFactory extends Configurable {
> 
>     HttpRequest createRequest(Offset offset);
> }
> ```
> *   Type: `Class`
> *   Default: `com.github.castorm.kafka.connect.http.request.template.TemplateHttpRequestFactory`
> *   Available implementations:
>     *   `com.github.castorm.kafka.connect.http.request.template.TemplateHttpRequestFactory`
>
> #### `http.offset.initial`
> Initial offset, comma separated list of pairs.
> *   Example: `property1=value1, property2=value2`
> *   Type: `String`
> *   Default: `""`

### Pagination

>
> #### `http.offset.nextpage`
> Which response offset property will hold the next page parameter to trigger subsequent calls. When set, the connector will loop through pages until the response no longer contains a next-page value.
> *   Example: `endCursor`
> *   Type: `String`
> *   Default: `""`
>
> #### `http.response.offset.pointer`
> Which offsets to pick from root response
> *   Example: `endCursor=/pagination/endCursor`
> *   Type: `String`
> *   Default: `""`
>

### Counter-Based Pagination

Numeric offset pagination for APIs that use a `limit` + `offset` (or `skip`) pattern, such as the [Workday WQL API](https://community.workday.com/sites/default/files/file-hosting/restapi/index.html#wql/v1/post-/data).
When `http.offset.counter` is set, the connector maintains an integer counter in the offset. After each page request, the counter is incremented by the number of records returned. The connector compares the total records fetched so far against a `total` field extracted from the response (configurable via `http.offset.counter.total.field`). If `fetched < total`, another request is issued with the updated counter. The counter is reset to `0` at the start of every poll and after all pages have been consumed so the next poll cycle starts fresh.

The per-poll pagination value is available as `${offset.pageOffset}` in request URL / query parameter / body templates. `pageOffset` is request-lifecycle state: it starts at `0` for each poll, advances only while fetching pages in that poll, and is reset before it is committed.

**Example flow** for a Workday WQL endpoint returning max 10 000 records:
1. Poll 1 – initial request without an offset parameter, response has `total=25000`, returns 10 000 records → counter becomes `10000`, loop continues.  
2. Poll 1 – request with `offset=10000`, returns 10 000 records → counter becomes `20000`, loop continues.  
3. Poll 1 – request with `offset=20000`, returns 5 000 records → `25000 >= 25000`, loop stops, counter is reset to `0`.  
4. Poll 2 – starts again with no offset parameter (or from a new auto-date window if combined with `http.offset.date_initial_date`).

>
> #### `http.offset.counter`
> Name of the offset field used to enable numeric page counting. Setting this property **enables** counter-based pagination. Use `${offset.pageOffset}` in request templates for the current per-poll page offset.
> *   Example: `offsetCounter`
> *   Type: `String`
> *   Default: `""` (disabled)
>
> #### `http.offset.counter.total.field`
> Name of the field in the parsed response offset that holds the total number of available records. Must be extracted from the response body via `http.response.offset.pointer`.
> *   Example: `total`
> *   Type: `String`
> *   Default: `total`
>
> #### `http.offset.counter.auth.restarts.max`
> Maximum number of times a counter-paginated snapshot may restart from offset `0` when authentication expires or refreshes. Partial records from the previous authentication generation are discarded because APIs such as Workday WQL may invalidate their pagination cache when issuing a new token.
> *   Type: `Integer`
> *   Default: `1`
>

**Minimal connector config (Workday WQL)**:
```properties
# Enable counter paging
http.offset.counter=offsetCounter
http.offset.counter.total.field=total
http.offset.counter.auth.restarts.max=1

# Extract "total" from the response root so the counter logic can read it
http.response.offset.pointer=total=/total

# Use pageOffset only after the initial request
http.request.url=https://<host>/ccx/api/wql/v1/{tenant}/data?limit=10000<#if offset.pageOffset gt 0>&offset=${offset.pageOffset}</#if>
http.request.body={"query": "SELECT workerId, ... FROM allWorkers WHERE ..."}
```

> **Note:** `http.offset.counter` and `http.offset.nextpage` are mutually exclusive. When both are set, cursor-based (`nextpage`) pagination takes priority.

### Auto-Date Increment
Automatically increments a date-based offset on each poll cycle, enabling time-windowed API queries. When `http.offset.date_initial_date` is set, the connector maintains an auto-incrementing timestamp in the offset. On each poll, the timestamp advances by `date_increment` milliseconds (minus `date_backoff` as a safety buffer), and is capped at the current time. This timestamp is available as `${offset.autotimestamp}` in request templates.

>
> #### `http.offset.date_initial_date`
> Initial date for the auto-incrementing offset, in ISO 8601 format. When set, enables the auto-date increment feature.
> *   Example: `2023-01-01T00:00:00`
> *   Type: `String`
> *   Default: `""`
>
> #### `http.offset.date_increment`
> Amount to increment the auto-date offset on each poll, in milliseconds.
> *   Example: `86400000` (1 day)
> *   Type: `String` (parsed as `Long`)
> *   Default: `""`
>
> #### `http.offset.date_backoff`
> Safety backoff subtracted from the auto-date increment on each poll, in milliseconds. Also used to cap the offset so it doesn't exceed `now - backoff`.
> *   Example: `1000` (1 second)
> *   Type: `String` (parsed as `Long`)
> *   Default: `""`
>


#### Creating a `HttpRequest` with `TemplateHttpRequestFactory`
This `HttpRequestFactory` is based on template resolution.

> ##### `http.request.method`
> Http method to use in the request.
> *   Type: `String`
> *   Default: `GET`
> 
> ##### `http.request.url`
> Http url to use in the request.
> *   __Required__
> *   Type: `String`
> 
> ##### `http.request.headers`
> Http headers to use in the request, `,` separated list of `:` separated pairs.
> *   Example: `Name: Value, Name2: Value2`
> *   Type: `String`
> *   Default: `""`
> 
> ##### `http.request.params`
> Http query parameters to use in the request, `&` separated list of `=` separated pairs.
> *   Example: `name=value & name2=value2`
> *   Type: `String`
> *   Default: `""`
> 
> ##### `http.request.body`
> Http body to use in the request.
> *   Type: `String`
> *   Default: `""`
> 
> ##### `http.request.template.factory`
> ```java
> public interface TemplateFactory {
> 
>     Template create(String template);
> }
> 
> public interface Template {
> 
>     String apply(Offset offset);
> }
> ```
> Class responsible for creating the templates that will be used on every request.
> *   Type: `Class`
> *   Default: `com.github.castorm.kafka.connect.http.request.template.freemarker.BackwardsCompatibleFreeMarkerTemplateFactory`
> *   Available implementations:
>     *   `com.github.castorm.kafka.connect.http.request.template.freemarker.BackwardsCompatibleFreeMarkerTemplateFactory`
          Implementation based on [FreeMarker](https://freemarker.apache.org/) which accepts offset properties without 
          `offset` namespace _(Deprecated)_
>     *   `com.github.castorm.kafka.connect.http.request.template.freemarker.FreeMarkerTemplateFactory`
          Implementation based on [FreeMarker](https://freemarker.apache.org/)
>     *   `com.github.castorm.kafka.connect.http.request.template.NoTemplateFactory`

##### Creating a `HttpRequest` with `FreeMarkerTemplateFactory`
FreeMarker templates will have the following data model available:
*   `offset`
    *   `key`
    *   `timestamp` (as ISO8601 string, e.g.: `2020-01-01T00:00:00Z`)
    *   ... _(custom offset properties)_

Accessing any of the above withing a template can be achieved like this:
```properties
http.request.params=after=${offset.timestamp}
```
For an Epoch representation of the same string, FreeMarker built-ins should be used:
```properties
http.request.params=after=${offset.timestamp?datetime.iso?long}
```
For a complete understanding of the features provided by FreeMarker, please, refer to the 
[User Manual](https://freemarker.apache.org/docs/index.html)

---
<a name="client"/>

### `HttpClient`: Executing a `HttpRequest`
Once our HttpRequest is ready, we have to execute it to get some results out of it. That's the purpose of the 
`HttpClient`

> #### `http.client`
> ```java
> public interface HttpClient extends Configurable {
> 
>     HttpResponse execute(HttpRequest request) throws IOException;
> }
> ```
> *   Type: `Class`
> *   Default: `com.github.castorm.kafka.connect.http.client.okhttp.OkHttpClient`
> *   Available implementations:
>     *   `com.github.castorm.kafka.connect.http.client.okhttp.OkHttpClient`

#### Executing a `HttpRequest` with `OkHttpClient`
Uses a [OkHttp](https://square.github.io/okhttp/) client. 

> ##### `http.client.connection.timeout.millis`
> Timeout for opening a connection
> *   Type: `Long`
> *   Default: `2000`
> 
> ##### `http.client.read.timeout.millis`
> Timeout for reading a response
> *   Type: `Long`
> *   Default: `2000`
> 
> ##### `http.client.ttl.millis`
> Keep alive duration for connections
> *   Type: `Long`
> *   Default: `300000`
> 
> ##### `http.client.max-idle`
> Maximum number of idle connections in the connection pool
> *   Type: `Integer`
> *   Default: `1`
> 
> ##### `http.client.proxy.host`
> Hostname of the HTTP Proxy
> *   Type: `String`
> *   Default: ``
> 
> ##### `http.client.proxy.port`
> Port of the HTTP Proxy
> *   Type: `Integer`
> *   Default: `3128`
> 
> ##### `http.client.proxy.username`
> Username of the HTTP Proxy
> *   Type: `String`
> *   Default: ``
> 
> ##### `http.client.proxy.password`
> Password of the HTTP Proxy
> *   Type: `String`
> *   Default: ``
>
> ##### `http.client.keystore`
> Path to a PKCS12/JKS keystore for client TLS certificates
> *   Type: `String`
> *   Default: `""`
>
> ##### `http.client.keystore.password`
> Password for the keystore
> *   Type: `Password`
> *   Default: `""`
---
<a name="auth"/>

### `HttpAuthenticator`: Authenticating a `HttpRequest`
When executing the request, authentication might be required. The `HttpAuthenticator` is responsible for resolving the `Authorization` header
to be included in the `HttpRequest`.

> #### `http.auth`
> ```java
> public interface HttpAuthenticator extends Configurable {
> 
>     Optional<String> getAuthorizationHeader();
> }
> ```
> *   Type: `Class`
> *   Default: `com.github.castorm.kafka.connect.http.auth.ConfigurableHttpAuthenticator`
> *   Available implementations:
>     *   `com.github.castorm.kafka.connect.http.auth.ConfigurableHttpAuthenticator`
>     *   `com.github.castorm.kafka.connect.http.auth.NoneHttpAuthenticator`
>     *   `com.github.castorm.kafka.connect.http.auth.BasicHttpAuthenticator`
>     *   `com.github.castorm.kafka.connect.http.auth.OAuthClientCredentialsAuthenticator`

#### Authenticating with `ConfigurableHttpAuthenticator`
Allows selecting the authentication type via configuration property

> ##### `http.auth.type`
> Type of authentication
> *   Type: `Enum { None, Basic, Token_Endpoint, Bearer, OAuth }`
> *   Default: `None`

#### Authenticating with `BasicHttpAuthenticator`
Allows selecting the authentication type via configuration property

> ##### `http.auth.user`
> *   Type: `String`
> *   Default: `""`
>
> ##### `http.auth.password`
> *   Type: `String`
> *   Default: `"""`

#### Authenticating with `TokenEndpointAuthenticator`
Fetches a Bearer token from an HTTP endpoint, caches it with automatic expiry management (via JWT `exp` claim or configurable fallback), and supports optional two-step chained authentication.

Set `http.auth.type=Token_Endpoint` to use this authenticator.

> ##### `http.auth.url`
> Token endpoint URL
> *   Type: `String`
> *   Default: `""`
>
> ##### `http.auth.method`
> HTTP method for the token request
> *   Type: `String`
> *   Default: `"POST"`
>
> ##### `http.auth.body`
> Request body (e.g. JSON or form-encoded payload) sent to the token endpoint
> *   Type: `Password`
> *   Default: `""`
>
> ##### `http.auth.tokenkeypath`
> JSON key to extract the access token from the response
> *   Type: `String`
> *   Default: `"access_token"`
>
> ##### `http.token.request.headers`
> HTTP headers for the token request as comma-separated `Key=Value` pairs
> *   Example: `Content-Type=application/x-www-form-urlencoded,Authorization=Basic dXNlcjpwYXNz`
> *   Type: `String`
> *   Default: `""`
>
> ##### `http.token.expiry.seconds`
> Fallback token expiry in seconds, used when the token is not a JWT with an `exp` claim
> *   Type: `Integer`
> *   Default: `3540` (59 minutes)
>
> ##### `http.auth.chain.url`
> Second-step chain endpoint URL. When set, enables two-step authentication: the first call fetches a refresh token, the second call uses it to get the access token.
> *   Type: `String`
> *   Default: `""`
>
> ##### `http.auth.chain.method`
> HTTP method for the chain request
> *   Type: `String`
> *   Default: `"GET"`
>
> ##### `http.auth.chain.token.key`
> JSON key to extract the intermediate token from the first-step response (used in chained auth)
> *   Type: `String`
> *   Default: `"refresh_token"`
>
> ##### `http.auth.chain.headers`
> HTTP headers for the chain request as comma-separated `Key=Value` pairs. Supports `{{token}}` placeholder which is replaced with the token from the first step.
> *   Type: `String`
> *   Default: `""`
>
> ##### `http.auth.chain.body`
> Request body for the chain request
> *   Type: `Password`
> *   Default: `""`

#### Authenticating with `BearerAuthenticator`
Uses a static Bearer token provided directly via configuration. No token refresh or endpoint calls.

Set `http.auth.type=Bearer` to use this authenticator.

> ##### `http.auth.bearer`
> The full `Authorization` header value (e.g. `Bearer eyJ...`)
> *   Type: `String`
> *   Default: `""`

#### Authenticating with `OAuthClientCredentialsAuthenticator`
Implements the OAuth2 Client Credentials grant flow. Automatically fetches and caches an access token from an OAuth2 token endpoint using `client_id`, `client_secret`, and optional `scope`. Tokens are cached and refreshed automatically based on JWT `exp` claim, the `expires_in` field in the response, or a configurable fallback expiry.

Set `http.auth.type=OAuth` to use this authenticator.

> ##### `http.auth.oauth.token.url`
> The OAuth2 token endpoint URL
> *   Type: `String`
> *   Default: `""`
>
> ##### `http.auth.oauth.client.id`
> OAuth2 client ID
> *   Type: `String`
> *   Default: `""`
>
> ##### `http.auth.oauth.client.secret`
> OAuth2 client secret
> *   Type: `Password`
> *   Default: `""`
>
> ##### `http.auth.oauth.scope`
> OAuth2 scopes (space-separated). If empty, scope is omitted from the token request.
> *   Type: `String`
> *   Default: `""`
>
> ##### `http.auth.oauth.method`
> HTTP method for the token request
> *   Type: `String`
> *   Default: `"POST"`
>
> ##### `http.auth.oauth.headers`
> HTTP headers for the token request as comma-separated `Key=Value` pairs
> *   Type: `String`
> *   Default: `"Content-Type=application/x-www-form-urlencoded,Accept=application/json"`
>
> ##### `http.auth.oauth.token.key`
> JSON key to extract the access token from the token endpoint response
> *   Type: `String`
> *   Default: `"access_token"`
>
> ##### `http.auth.oauth.grant.type`
> OAuth2 grant type
> *   Type: `String`
> *   Default: `"client_credentials"`
>
> ##### `http.auth.oauth.token.expiry.seconds`
> Fallback token expiry in seconds, used when the response does not include `expires_in` and the token is not a JWT with an `exp` claim
> *   Type: `Integer`
> *   Default: `3540` (59 minutes)

**Example configuration:**
```properties
http.auth.type=OAuth
http.auth.oauth.token.url=https://api.example.com/identity/connect/token
http.auth.oauth.client.id=my-client-id
http.auth.oauth.client.secret=my-client-secret
http.auth.oauth.scope=myapi:read
```

---
<a name="response"/>

### `HttpResponseParser`: Parsing a `HttpResponse`
Once our `HttpRequest` has been executed, as a result we'll have to deal with a `HttpResponse` and translate it into 
the list of `SourceRecord`s expected by Kafka Connect. 

> #### `http.response.parser`
> ```java
> public interface HttpResponseParser extends Configurable {
> 
>     List<SourceRecord> parse(HttpResponse response);
> }
> ```
> *   Type: `Class`
> *   Default: `com.github.castorm.kafka.connect.http.response.PolicyHttpResponseParser`
> *   Available implementations:
>     *   `com.github.castorm.kafka.connect.http.response.PolicyHttpResponseParser`
>     *   `com.github.castorm.kafka.connect.http.response.KvHttpResponseParser`

#### Parsing with `PolicyHttpResponseParser`
Vets the HTTP response deciding whether the response should be processed, skipped or failed. This decision is delegated
to a `HttpResponsePolicy`. 
When the decision is to process the response, this processing is delegated to a secondary `HttpResponseParser`.

##### `HttpResponsePolicy`: Vetting a `HttpResponse`

> ##### `http.response.policy`
> ```java
> public interface HttpResponsePolicy extends Configurable {
> 
>     HttpResponseOutcome resolve(HttpResponse response);
> 
>     enum HttpResponseOutcome {
>         PROCESS, SKIP, FAIL
>     }
> }
> ```
> *   Type: `Class`
> *   Default: `com.github.castorm.kafka.connect.http.response.StatusCodeHttpResponsePolicy`
> *   Available implementations:
>     *   `com.github.castorm.kafka.connect.http.response.StatusCodeHttpResponsePolicy`
>
> ##### `http.response.policy.parser`
> *   Type: `Class`
> *   Default: `com.github.castorm.kafka.connect.http.response.KvHttpResponseParser`
> *   Available implementations:
>     *   `com.github.castorm.kafka.connect.http.response.KvHttpResponseParser`

###### Vetting with `StatusCodeHttpResponsePolicy`
Does response vetting based on HTTP status codes in the response and the configuration below.

> ##### `http.response.policy.codes.process`
> Comma separated list of code ranges that will result in the parser processing the response
> *   Example: `200..205, 207..210`
> *   Type: `String`
> *   Default: `200..299`
>
> ##### `http.response.policy.codes.skip`
> Comma separated list of code ranges that will result in the parser skipping the response
> *   Example: `300..305, 307..310`
> *   Type: `String`
> *   Default: `300..399`

#### Parsing with `KvHttpResponseParser`
Parses the HTTP response into a key-value SourceRecord. This process is decomposed in two steps:
*   Parsing the `HttpResponse` into a `KvRecord`
*   Mapping the `KvRecord` into a `SourceRecord`

> ##### `http.response.record.parser`
> ```java
> public interface KvRecordHttpResponseParser extends Configurable {
> 
>     List<KvRecord> parse(HttpResponse response);
> }
> ```
> *   Type: `Class`
> *   Default: `com.github.castorm.kafka.connect.http.response.jackson.JacksonKvRecordHttpResponseParser`
> *   Available implementations:
>     *   `com.github.castorm.kafka.connect.http.response.jackson.JacksonKvRecordHttpResponseParser`
>
> ##### `http.response.record.mapper`
> ```java
> public interface KvSourceRecordMapper extends Configurable {
> 
>     SourceRecord map(KvRecord record);
> }
> ```
> *   Type: `Class`
> *   Default: `com.github.castorm.kafka.connect.http.record.SchemedKvSourceRecordMapper`
> *   Available implementations:
>     *   `com.github.castorm.kafka.connect.http.record.SchemedKvSourceRecordMapper`
>         Maps __key__ to a *Struct schema* with a single property `key`, and __value__ to a *Struct schema* with a single property `value`
>     *   `com.github.castorm.kafka.connect.http.record.StringKvSourceRecordMapper`
>         Maps both __key__ and __value__ to a `String` schema

##### Parsing with `JacksonKvRecordHttpResponseParser`
Uses [Jackson](https://github.com/FasterXML/jackson) to look for the records in the response.

> ##### `http.response.list.pointer`
> [JsonPointer](https://tools.ietf.org/html/rfc6901) to the property in the response body containing an array of records 
> *   Example: `/items`
> *   Type: `String`
> *   Default: `/`
> 
> ##### `http.response.record.pointer`
> [JsonPointer](https://tools.ietf.org/html/rfc6901) to the individual record to be used as kafka record body. Useful
  when the object we are interested in is under a nested structure
> *   Type: `String`
> *   Default: `/`
>
> ##### `http.response.record.offset.pointer`
> Comma separated list of `key=/value` pairs where the key is the name of the property in the offset, and the value is
> the [JsonPointer](https://tools.ietf.org/html/rfc6901) to the value being used as offset for future requests.
> This is the mechanism that enables sharing state in between `HttpRequests`. `HttpRequestFactory` implementations 
> receive this `Offset`.
>
> Special properties:
> - `key` is used as record's identifier, used for *de-duplication* and topic *partition routing*
> - `timestamp` is used as record's timestamp, used for *de-duplication* and *ordering*
> 
> One of the roles of the offset, even if not required for preparing the next request, is helping in deduplication of
> already seen records, by providing a sense of progress, assuming consistent ordering. (e.g. even if the response returns
> some repeated results in between requests because they have the same timestamp, anything prior to the last seen
> offset will be ignored). see `OffsetFilterFactory`
> *   Example: `id=/itemId`
> *   Type: `String`
> *   Default: `""`
>
> ##### `http.response.record.timestamp.parser`
> Class responsible for converting the timestamp property captured above into a `java.time.Instant`.  
> *   Type: `String`
> *   Default: `com.github.castorm.kafka.connect.http.response.timestamp.EpochMillisOrDelegateTimestampParser`
> *   Available implementations:
>     *   `com.github.castorm.kafka.connect.http.response.timestamp.EpochMillisTimestampParser` 
           Implementation that captures the timestamp as an epoch millis long
>     *   `com.github.castorm.kafka.connect.http.response.timestamp.EpochMillisOrDelegateTimestampParser` 
           Implementation that tries to capture as epoch millis or delegates to another parser in case of failure
>     *   `com.github.castorm.kafka.connect.http.response.timestamp.DateTimeFormatterTimestampParser` 
           Implementation based on based on a `DateTimeFormatter`
>     *   `com.github.castorm.kafka.connect.http.response.timestamp.NattyTimestampParser`
           Implementation based on [Natty](http://natty.joestelmach.com/) parser
>     *   `com.github.castorm.kafka.connect.http.response.timestamp.RegexTimestampParser`
          Implementation that extracts substring from timestamp column and parse it 

> ##### `http.response.record.timestamp.parser.pattern`
> When using `DateTimeFormatterTimestampParser`, a custom pattern can be specified 
> *   Type: `String`
> *   Default: `yyyy-MM-dd'T'HH:mm:ss[.SSS]X`
> 
> ##### `http.response.record.timestamp.parser.zone`
> Timezone of the timestamp. Accepts [ZoneId](https://docs.oracle.com/javase/8/docs/api/java/time/ZoneId.html) valid
  identifiers
> *   Type: `String`
> *   Default: `UTC`
>
> ##### `http.response.record.timestamp.parser.regex`
> When using `RegexTimestampParser`, a custom regex pattern can be specified 
> *   Type: `String`
> *   Default: `.*`
>
##### `http.response.record.timestamp.parser.regex.delegate`
> When using `RegexTimestampParser`, a delegate class to parse timestamp 
> *   Type: `Class`
> *   Default: `DateTimeFormatterTimestampParser`

---
<a name="mapper"/>

### Mapping a `KvRecord` into `SourceRecord` with `SimpleKvSourceRecordMapper`
Once we have our `KvRecord` we have to translate it into what Kafka Connect is expecting: `SourceRecord`s

Embeds the record properties into a common simple envelope to enable schema evolution. This envelope simply contains
a key and a value properties with customizable field names.

Here is also where we'll tell Kafka Connect to what topic and on what partition do we want to send our record.

** It's worth noticing there are projects out there that allow you to infer the schema from your json document.
(e.g. [expandjsonsmt](https://github.com/RedHatInsights/expandjsonsmt))

> ##### `kafka.topic`
> Name of the topic where the record will be sent to
> *   __Required__
> *   Type: `String`
> *   Default: `""`
>
> ##### `http.record.schema.key.property.name`
> Name of the key property in the key-value envelope
> *   Type: `String`
> *   Default: `key`
>
> ##### `http.record.schema.value.property.name`
> Name of the value property in the key-value envelope
> *   Type: `String`
> *   Default: `value`

---
<a name="sorter"/>

### SourceRecordSorter: Sorting SourceRecords
Some Http resources not designed for CDC, return snapshots with most recent records first. In this cases de-duplication
is especially important, as subsequent request are likely to produce similar results. The de-duplication mechanisms 
offered by this connector are order-dependent, as they are usually based on timestamps.

To enable de-duplication in cases like this, we can instruct the connector to assume a specific order direction, either
`ASC`, `DESC`, or `IMPLICIT`, where implicit figures it out based on records' timestamps.

> #### `http.record.sorter`
> ```java
> public interface SourceRecordSorter extends Configurable {
> 
>     List<SourceRecord> sort(List<SourceRecord> records);
> }
> ```
> *   Type: `Class`
> *   Default: `com.github.castorm.kafka.connect.http.record.OrderDirectionSourceRecordSorter`
> *   Available implementations:
>     *   `com.github.castorm.kafka.connect.http.record.OrderDirectionSourceRecordSorter`
>
> #### `http.response.list.order.direction`
> Order direction of the results in the response list.
> *   Type: `Enum { ASC, DESC, IMPLICIT }`
> *   Default: `IMPLICIT`

---
<a name="filter"/>

### SourceRecordFilterFactory: Filtering out SourceRecord
There are cases when we'll be interested in filtering out certain records. One of these would be de-duplication.

> #### `http.record.filter.factory`
> ```java
> public interface SourceRecordFilterFactory extends Configurable {
> 
>     Predicate<SourceRecord> create(Offset offset);
> }
> ```
> *   Type: `Class`
> *   Default: `com.github.castorm.kafka.connect.http.record.OffsetRecordFilterFactory`
> *   Available implementations:
>     *   `com.github.castorm.kafka.connect.http.record.OffsetRecordFilterFactory`
>     *   `com.github.castorm.kafka.connect.http.record.OffsetTimestampRecordFilterFactory`
>     *   `com.github.castorm.kafka.connect.http.record.PassthroughRecordFilterFactory`

#### Filtering out SourceRecord with OffsetTimestampRecordFilterFactory

De-duplicates based on `Offset`'s timestamp, filtering out records with earlier or the same timestamp. 
Useful when timestamp is used to filter the HTTP resource, but the filter does not have full timestamp precision.
Assumptions:
*   Records are ordered by timestamp
*   No two records can contain the same timestamp (to whatever precision the HTTP resource uses)

If the latter assumption cannot be satisfied, check `OffsetRecordFilterFactory` to try and prevents data loss.

#### Filtering out SourceRecord with OffsetRecordFilterFactory

De-duplicates based on `Offset`'s timestamp, key and any other custom property present in the `Offset`, filtering out 
records with earlier timestamps, or when in the same timestamp, only those up to the last seen `Offset` properties.
Useful when timestamp alone is not unique but together with some other `Offset` property is.
Assumptions: 
*   Records are ordered by timestamp
*   There is an `Offset` property that uniquely identify records (e.g. key)
*   There won't be new items preceding already seen ones 

---
## Development

### Building
```bash
mvn package
```
### Debugging

#### Using Pre-configured docker setup

You can easily run a Kafka Connect cluster with kafka-connect-http pre-installed by executing:
```bash
mvn verify -Pdebug -DskipTests
```
It'll run dockerized versions of kafka and kafka-connect which you can access via REST API or attach debuggers to the url printed in
console:
```bash
Kafka Connect testcontainers infra is ready
  Rest API: http://localhost:33216
  Debug agent: localhost:33217
```
Right after, it'll allow you to specify the file path to your connector's json configuration:
```bash:
Introduce the path to your connector JSON configuration file:
```
It'll subscribe to the corresponding kafka topic, printing every message going through the output topic of your connector.

#### Using Kafka Connect standalone
_These instructions are phrased in terms of the steps needed when using IntelliJ,
but other integrated development environments are likely to be similar._

Point the Kafka stand-alone **plugin.path** at the module compile **Output path**.
Assuming you are using the default Maven project import,
this is the **./target** directory, so the **config/connect-standalone.properties** file would contain the line
```bash
plugin.path=<directory where git clone was executed>/kafka-connect-http/kafka-connect-http/target
```
In the **Run/Debug Configurations** dialog, create a new **Remote JVM Debug** configuration with the mode **Attach to remote JVM**.
When remote debugging, some Java parameters need to be specified when the program is executed.
Fortunately there are hooks in the Kafka shell scripts to accommodate this.
The **Remote JVM Debug** configuration specifies the needed **Command line arguments for remote JVM**.
In the terminal console where you execute the connect command line, define **KAFKA_DEBUG** and **JAVA_DEBUG_OPTS** as:
```bash
export KAFKA_DEBUG=true
export JAVA_DEBUG_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005
```
Place a suitable breakpoint in the kafka-connect-http code, e.g. in `HttpSourceTask.start()`, and launch the standalone connect program:
```bash
bin/connect-standalone.sh config/connect-standalone.properties plugins/<kafka-connect-http properties file>
```
Click the Debug icon in IntelliJ and ensure
the debugger console says `Connected to the target VM, address: 'localhost:5005', transport: 'socket'`
and the breakpoint you placed becomes checked.
The program should now break when the breakpoint is hit.
### Running the tests
```bash
mvn test
```
### Releasing
*   Update [CHANGELOG.md](CHANGELOG.md) and [README.md](README.md) files.
*   Prepare release:
```bash
mvn release:clean release:prepare
```

## Contributing

Contributions are welcome via pull requests, pending definition of code of conduct, please just follow existing conventions.

## Versioning

We use [SemVer](http://semver.org/) for versioning. 

## License

This project is licensed under the Apache 2.0 License - see the [LICENSE.txt](LICENSE.txt) file for details

## Built With

*   [Maven](https://maven.apache.org/) - Dependency Management
*   [Kafka Connect](https://kafka.apache.org/documentation/#connect) - The framework for our connectors
*   [OkHttp](https://square.github.io/okhttp/) - HTTP Client
*   [Jackson](https://github.com/FasterXML/jackson) - Json deserialization
*   [FreeMarker](https://freemarker.apache.org/) - Template engine
*   [Natty](http://natty.joestelmach.com/) - Date parser

## Acknowledgments

*   Inspired by [llofberg/kafka-connect-rest](https://github.com/llofberg/kafka-connect-rest)
