/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jp.co.yahoo.presto.audit;

import org.apache.pulsar.client.api.PulsarClientException;
import org.testng.annotations.Test;

import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@Test(singleThreaded = true)
public class TestAuditLogListener
{
    TestHelper testHelper = new TestHelper();

    private AuditLogListener createSimpleAuditLogListener(AuditLogFileWriter auditLogFileWriter)
            throws PulsarClientException
    {
        AuditConfig config = new AuditConfig()
                .setAuditLogFileWriter(auditLogFileWriter)
                .setAuditLogPath("/test/path")
                .setAuditSimpleLogName("test-filename.log");
        return new AuditLogListener(config);
    }

    private AuditLogListener createFullAuditLogListener(AuditLogFileWriter auditLogFileWriter)
            throws PulsarClientException
    {
        AuditConfig config = new AuditConfig()
                .setAuditLogFileWriter(auditLogFileWriter)
                .setAuditLogPath("/test/path_full")
                .setAuditSimpleLogName("test-filename.log")
                .setAuditFullLogName("test-filename-full.log");
        return new AuditLogListener(config);
    }

    private AuditLogListener createFullAuditLogListenerWithFilter(AuditLogFileWriter auditLogFileWriter, String filter)
            throws PulsarClientException
    {
        AuditConfig config = new AuditConfig()
                .setAuditLogFileWriter(auditLogFileWriter)
                .setAuditLogPath("/test/path_full")
                .setAuditSimpleLogName("test-filename.log")
                .setAuditFullLogName("test-filename-full.log")
                .setLogFilter(filter);
        return new AuditLogListener(config);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testCreateAuditLogListenerEmpty()
            throws PulsarClientException
    {
        AuditConfig config = new AuditConfig();
        new AuditLogListener(config);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testCreateAuditLogListenerMissingPath()
            throws PulsarClientException
    {
        AuditConfig config = new AuditConfig()
                .setAuditSimpleLogName("test-filename.log");
        new AuditLogListener(config);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testCreateAuditLogListenerMissingFilename()
            throws PulsarClientException
    {
        AuditConfig config = new AuditConfig()
                .setAuditLogPath("/test/path");
        new AuditLogListener(config);
    }

    @Test
    public void testCreateAuditLogListenerSuccessWithoutFullFilename()
            throws PulsarClientException
    {
        createSimpleAuditLogListener(AuditLogFileWriter.getInstance());
    }

    @Test
    public void testCreateAuditLogListenerSuccessWithFullFilename()
            throws PulsarClientException
    {
        createFullAuditLogListener(AuditLogFileWriter.getInstance());
    }

    @Test
    public void testSimpleLogQueryCompleted()
            throws PulsarClientException
    {
        AuditLogFileWriter auditLogFileWriterMock = mock(AuditLogFileWriter.class);
        AuditLogListener auditLogListener = createSimpleAuditLogListener(auditLogFileWriterMock);

        auditLogListener.queryCompleted(testHelper.createNormalEvent());
        verify(auditLogFileWriterMock, times(1)).write(eq("/test/path/test-filename.log"), any());
    }

    @Test
    public void testFullLogQueryCompleted()
            throws PulsarClientException
    {
        AuditLogFileWriter auditLogFileWriterMock = mock(AuditLogFileWriter.class);
        AuditLogListener auditLogListener = createFullAuditLogListener(auditLogFileWriterMock);

        auditLogListener.queryCompleted(testHelper.createNormalEvent());
        verify(auditLogFileWriterMock, times(1)).write(eq("/test/path_full/test-filename.log"), any());
        verify(auditLogFileWriterMock, times(1)).write(eq("/test/path_full/test-filename-full.log"), any());
    }

    @Test
    public void testFilter()
            throws PulsarClientException
    {
        AuditLogFileWriter auditLogFileWriterMock = mock(AuditLogFileWriter.class);
        AuditLogListener auditLogListener = createFullAuditLogListenerWithFilter(auditLogFileWriterMock, "SRE_SYSTEM");

        auditLogListener.queryCompleted(testHelper.createNormalEvent());
        verify(auditLogFileWriterMock, times(1)).write(eq("/test/path_full/test-filename.log"), any());
        verify(auditLogFileWriterMock, times(1)).write(eq("/test/path_full/test-filename-full.log"), any());

        auditLogListener.queryCompleted(testHelper.createQueryWithSource(Optional.of("SRE_SYSTEM")));
        verify(auditLogFileWriterMock, times(2)).write(eq("/test/path_full/test-filename.log"), any());
        verify(auditLogFileWriterMock, times(1)).write(eq("/test/path_full/test-filename-full.log"), any());
    }

    @Test
    public void testFilterComplex()
            throws PulsarClientException
    {
        AuditLogFileWriter auditLogFileWriterMock = mock(AuditLogFileWriter.class);
        AuditLogListener auditLogListener = createFullAuditLogListenerWithFilter(auditLogFileWriterMock, "(SRE_SYSTEM|Presto_team)");

        auditLogListener.queryCompleted(testHelper.createNormalEvent());
        verify(auditLogFileWriterMock, times(1)).write(eq("/test/path_full/test-filename.log"), any());
        verify(auditLogFileWriterMock, times(1)).write(eq("/test/path_full/test-filename-full.log"), any());

        auditLogListener.queryCompleted(testHelper.createQueryWithSource(Optional.of("SRE_SYSTEM")));
        verify(auditLogFileWriterMock, times(2)).write(eq("/test/path_full/test-filename.log"), any());
        verify(auditLogFileWriterMock, times(1)).write(eq("/test/path_full/test-filename-full.log"), any());

        auditLogListener.queryCompleted(testHelper.createQueryWithSource(Optional.of("Presto_team")));
        verify(auditLogFileWriterMock, times(3)).write(eq("/test/path_full/test-filename.log"), any());
        verify(auditLogFileWriterMock, times(1)).write(eq("/test/path_full/test-filename-full.log"), any());
    }
}
