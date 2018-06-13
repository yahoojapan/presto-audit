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
package jp.co.yahoo.presto.audit.serializer;

import jp.co.yahoo.presto.audit.TestHelper;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true)
public class TestSimpleLogSerializer
{
    private SimpleLogSerializer simpleLogSerializer = new SimpleLogSerializer();
    private TestHelper testHelper = new TestHelper();

    @Test
    public void testSerializeNormal()
    {
        SerializedLog record = simpleLogSerializer.serialize(testHelper.createNormalEvent());
        assertThat(record.getQueryId())
                .matches("20170606_044544_00024_nfhe3");
        assertThat(record.getSerializedLog())
                .contains("\"queryId\":\"20170606_044544_00024_nfhe3\"")
                .contains("\"query\":\"select * from airdelays_s3_csv WHERE kw = 'presto-kw-example' limit 5\"")
                .contains("\"userAgent\":\"StatementClient 0.167\"")
                .contains("\"source\":\"presto-cli\"")
                .contains("\"errorCode\":0")
                .contains("\"createTimestamp\":1.5000804E9")
                .contains("\"executionStartTimestamp\":1.500080401E9")
                .contains("\"endTimestamp\":1.500080403E9")
                .contains("\"eventType\":\"QueryCompletedEvent\"")
                .doesNotContain("\"failureMessage\"")
                .doesNotContain("\"errorName\"");
    }

    @Test
    public void testSerializeFailure()
    {
        SerializedLog record = simpleLogSerializer.serialize(testHelper.createFailureEvent());
        assertThat(record.getSerializedLog())
                .contains("\"errorCode\":1")
                .contains("\"errorName\":\"SYNTAX_ERROR\"")
                .contains("\"failureMessage\":\"line 1:15: mismatched input '0' expecting ')'\"")
                .contains("\"failureType\":\"com.facebook.presto.sql.parser.ParsingException\"");
    }
}
