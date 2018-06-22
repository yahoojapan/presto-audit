/**
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
import com.facebook.presto.spi.eventlistener.StageCpuDistribution;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Optional;

public class TestHelper
{
    private URI uri;
    private QueryStatistics statistics;
    private QueryContext context;
    private QueryIOMetadata ioMetadata;
    private Instant createTime;
    private Instant executionStartTime;
    private Instant endTime;

    public void setUp()
    {
        try {
            uri = new URI("http://example.com:8080/v1/query/20170521_140224_00002_gd5k3");
            statistics = new QueryStatistics(Duration.ofMillis(100),
                    Duration.ofMillis(200),
                    Duration.ofMillis(300),
                    Optional.of(Duration.ofMillis(400)),
                    Optional.of(Duration.ofMillis(500)),
                    10001,
                    10002,
                    10003,
                    0,
                    14,
                    0,
                    0,
                    0,
                    0,
                    2048.0,
                    new ArrayList<>(),
                    4096,
                    true,
                    new ArrayList<StageCpuDistribution>() {},
                    new ArrayList<String>()
                    {{
                        add("operatorSummaries 01");
                    }}
            );
            context = new QueryContext(
                    "test-user",
                    Optional.of("principal"),
                    Optional.of("example.com"),
                    Optional.of("StatementClient 0.167"),
                    Optional.of("clientInfo"),
                    new HashSet<>(),
                    Optional.of("presto-cli"),
                    Optional.of("catalog"),
                    Optional.of("schema"),
                    Optional.of(""),
                    new HashMap<>(),
                    "127.0.0.1",
                    "0.175",
                    "environment");
            ioMetadata = new QueryIOMetadata(new ArrayList<QueryInputMetadata>(),
                    Optional.empty());
            ZoneId jst_zone = ZoneId.of("Asia/Tokyo");
            createTime = ZonedDateTime.of(2017, 6 + 1, 15, 10, 0, 0, 0, jst_zone).toInstant();
            executionStartTime = ZonedDateTime.of(2017, 6 + 1, 15, 10, 0, 1, 0, jst_zone).toInstant();
            endTime = ZonedDateTime.of(2017, 6 + 1, 15, 10, 0, 3, 0, jst_zone).toInstant();
        }
        catch (Exception ignoreException) {
        }
    }

    public QueryCompletedEvent createNormalEvent()
    {
        setUp();
        QueryMetadata metadata = new QueryMetadata("20170606_044544_00024_nfhe3",
                Optional.of("4c52973c-14c6-4534-837f-238e21d9b061"),
                "select * from airdelays_s3_csv WHERE kw = 'presto-kw-example' limit 5",
                "FINISHED",
                uri,
                Optional.empty(),
                Optional.empty());
        return new QueryCompletedEvent(metadata,
                statistics,
                context,
                ioMetadata,
                Optional.empty(),
                createTime,
                executionStartTime,
                endTime);
    }

    public QueryCompletedEvent createFailureEvent()
    {
        setUp();
        QueryMetadata metadata = new QueryMetadata("20170606_044544_00024_nfhe3",
                Optional.of("4c52973c-14c6-4534-837f-238e21d9b061"),
                "select 2a",
                "FAILED",
                uri,
                Optional.empty(),
                Optional.empty());
        ErrorCode errorCode = new ErrorCode(1, "SYNTAX_ERROR", ErrorType.USER_ERROR);
        QueryFailureInfo failureInfo = new QueryFailureInfo(errorCode,
                Optional.of("com.facebook.presto.sql.parser.ParsingException"),
                Optional.of("line 1:15: mismatched input '0' expecting ')'"),
                Optional.empty(),
                Optional.empty(),
                "{json-error}"
        );
        return new QueryCompletedEvent(metadata,
                statistics,
                context,
                ioMetadata,
                Optional.of(failureInfo),
                createTime,
                executionStartTime,
                endTime);
    }

    public QueryCompletedEvent createQueryWithSource(Optional<String> source)
    {
        setUp();
        QueryMetadata metadata = new QueryMetadata("20170606_044544_00024_nfhe3",
                Optional.of("4c52973c-14c6-4534-837f-238e21d9b061"),
                "select * from airdelays_s3_csv WHERE kw = 'presto-kw-example' limit 5",
                "FINISHED",
                uri,
                Optional.empty(),
                Optional.empty());
        QueryContext context = new QueryContext(
                "test-user",
                Optional.of("principal"),
                Optional.of("example.com"),
                Optional.of("StatementClient 0.167"),
                Optional.of("clientInfo"),
                new HashSet<>(),
                source,
                Optional.of("catalog"),
                Optional.of("schema"),
                Optional.of(""),
                new HashMap<>(),
                "127.0.0.1",
                "0.175",
                "environment");
        return new QueryCompletedEvent(metadata,
                statistics,
                context,
                ioMetadata,
                Optional.empty(),
                createTime,
                executionStartTime,
                endTime);
    }
}
