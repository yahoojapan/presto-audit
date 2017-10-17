/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jp.co.yahoo.presto.audit;

import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.ErrorType;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryContext;
import com.facebook.presto.spi.eventlistener.QueryFailureInfo;
import com.facebook.presto.spi.eventlistener.QueryIOMetadata;
import com.facebook.presto.spi.eventlistener.QueryInputMetadata;
import com.facebook.presto.spi.eventlistener.QueryMetadata;
import com.facebook.presto.spi.eventlistener.QueryStatistics;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.testng.annotations.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true)
public class TestAuditLogListener
{
    @Test
    public void testBuildAuditRecord()
            throws URISyntaxException
    {
        URI uri = new URI("http://example.com:8080/v1/query/20170521_140224_00002_gd5k3");
        QueryMetadata metadata = new QueryMetadata("20170606_044544_00024_nfhe3",
                Optional.of("4c52973c-14c6-4534-837f-238e21d9b061"),
                "select * from airdelays_s3_csv WHERE kw = 'presto-kw-example' limit 5",
                "FINISHED",
                uri,
                Optional.empty());
        QueryStatistics statistics = new QueryStatistics(Duration.ofMillis(100),
                Duration.ofMillis(200),
                Duration.ofMillis(300),
                Optional.of(Duration.ofMillis(400)),
                Optional.of(Duration.ofMillis(500)),
                10001,
                10002,
                10003,
                14,
                true,
                "operatorSummaries 01");
        QueryContext context = new QueryContext("test-user",
                Optional.of("principal"),
                Optional.of("example.com"),
                Optional.of("StatementClient 0.167"),
                Optional.of("clientInfo"),
                Optional.of("presto-cli"),
                Optional.of("catalog"),
                Optional.of("schema"),
                new HashMap<String, String>(),
                "127.0.0.1",
                "0.167t",
                "environment");
        QueryIOMetadata ioMetadata = new QueryIOMetadata(new ArrayList<QueryInputMetadata>(),
                Optional.empty());
        Instant createTime = Instant.ofEpochMilli(new GregorianCalendar(2017, 6, 15, 10, 0, 0).getTimeInMillis());
        Instant executionStartTime = Instant.ofEpochMilli(new GregorianCalendar(2017, 6, 15, 10, 0, 1).getTimeInMillis());
        Instant endTime = Instant.ofEpochMilli(new GregorianCalendar(2017, 6, 15, 10, 0, 3).getTimeInMillis());
        QueryCompletedEvent queryCompletedEvent = new QueryCompletedEvent(metadata,
                statistics,
                context,
                ioMetadata,
                Optional.empty(),
                createTime,
                executionStartTime,
                endTime);

        Map<String, String> requiredConfig = new HashMap<String, String>();
        requiredConfig.put("event-listener.audit-log-path", "/test/path");
        requiredConfig.put("event-listener.audit-log-filename", "test-filename.log");
        AuditLogListener listener = new AuditLogListener(requiredConfig);
        AuditRecord record = listener.buildAuditRecord(queryCompletedEvent);
        Gson obj = new GsonBuilder().disableHtmlEscaping().create();

        // Normal
        String expected_normal = "{\"queryId\":\"20170606_044544_00024_nfhe3\",\"query\":\"select * from airdelays_s3_csv WHERE kw = 'presto-kw-example' limit 5\",\"uri\":\"http://example.com:8080/v1/query/20170521_140224_00002_gd5k3\",\"state\":\"FINISHED\",\"cpuTime\":0.1,\"wallTime\":0.2,\"queuedTime\":0.3,\"peakMemoryBytes\":10001,\"totalBytes\":10002,\"totalRows\":10003,\"completedSplits\":14,\"createTime\":\"20170715100000.000\",\"executionStartTime\":\"20170715100001.000\",\"endTime\":\"20170715100003.000\",\"remoteClientAddress\":\"example.com\",\"clientUser\":\"test-user\",\"userAgent\":\"StatementClient 0.167\",\"source\":\"presto-cli\"}";
        assertThat(obj.toJson(record)).isEqualTo(expected_normal);

        // Failure
        metadata = new QueryMetadata("20170606_044544_00024_nfhe3",
                Optional.of("4c52973c-14c6-4534-837f-238e21d9b061"),
                "select 2a",
                "FAILED",
                uri,
                Optional.empty());
        ErrorCode errorCode = new ErrorCode(1, "SYNTAX_ERROR", ErrorType.USER_ERROR);
        QueryFailureInfo failureInfo = new QueryFailureInfo(errorCode,
                Optional.of("com.facebook.presto.sql.parser.ParsingException"),
                Optional.of("line 1:15: mismatched input '0' expecting ')'"),
                Optional.empty(),
                Optional.empty(),
                "{json-error}"
        );
        queryCompletedEvent = new QueryCompletedEvent(metadata,
                statistics,
                context,
                ioMetadata,
                Optional.of(failureInfo),
                createTime,
                executionStartTime,
                endTime);
        record = listener.buildAuditRecord(queryCompletedEvent);
        String expected_failure = "{\"queryId\":\"20170606_044544_00024_nfhe3\",\"query\":\"select 2a\",\"uri\":\"http://example.com:8080/v1/query/20170521_140224_00002_gd5k3\",\"state\":\"FAILED\",\"cpuTime\":0.1,\"wallTime\":0.2,\"queuedTime\":0.3,\"peakMemoryBytes\":10001,\"totalBytes\":10002,\"totalRows\":10003,\"completedSplits\":14,\"createTime\":\"20170715100000.000\",\"executionStartTime\":\"20170715100001.000\",\"endTime\":\"20170715100003.000\",\"errorCode\":\"SYNTAX_ERROR:1\",\"failureType\":\"com.facebook.presto.sql.parser.ParsingException\",\"failureMessage\":\"line 1:15: mismatched input '0' expecting ')'\",\"failuresJson\":\"{json-error}\",\"remoteClientAddress\":\"example.com\",\"clientUser\":\"test-user\",\"userAgent\":\"StatementClient 0.167\",\"source\":\"presto-cli\"}";
        assertThat(obj.toJson(record)).isEqualTo(expected_failure);
    }
}
