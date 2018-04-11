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

import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@Test(singleThreaded = true)
public class TestAuditLogListener
{
    TestHelper testHelper = new TestHelper();

    private AuditLogListener createSimpleAuditLogListener(AuditLogFileWriter auditLogFileWriter)
    {
        Map<String, String> requiredConfig = new HashMap<>();
        requiredConfig.put("event-listener.audit-log-path", "/test/path");
        requiredConfig.put("event-listener.audit-log-filename", "test-filename.log");
        return new AuditLogListener(requiredConfig, auditLogFileWriter);
    }

    private AuditLogListener createFullAuditLogListener(AuditLogFileWriter auditLogFileWriter)
    {
        Map<String, String> requiredConfig = new HashMap<>();
        requiredConfig.put("event-listener.audit-log-path", "/test/path_full");
        requiredConfig.put("event-listener.audit-log-filename", "test-filename.log");
        requiredConfig.put("event-listener.audit-log-full-filename", "test-filename-full.log");
        return new AuditLogListener(requiredConfig, auditLogFileWriter);
    }

    private AuditLogListener createFullAuditLogListenerWithFilter(AuditLogFileWriter auditLogFileWriter, String filter)
    {
        Map<String, String> requiredConfig = new HashMap<>();
        requiredConfig.put("event-listener.audit-log-path", "/test/path_full");
        requiredConfig.put("event-listener.audit-log-filename", "test-filename.log");
        requiredConfig.put("event-listener.audit-log-full-filename", "test-filename-full.log");
        requiredConfig.put("event-listener.audit-log-full-filter", filter);
        return new AuditLogListener(requiredConfig, auditLogFileWriter);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testCreateAuditLogListenerEmpty()
    {
        Map<String, String> requiredConfig = new HashMap<>();
        new AuditLogListener(requiredConfig, AuditLogFileWriter.getInstance());
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testCreateAuditLogListenerMissingPath()
    {
        Map<String, String> requiredConfig = new HashMap<>();
        requiredConfig.put("event-listener.audit-log-filename", "test-filename.log");
        new AuditLogListener(requiredConfig, AuditLogFileWriter.getInstance());
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testCreateAuditLogListenerMissingFilename()
    {
        Map<String, String> requiredConfig = new HashMap<>();
        requiredConfig.put("event-listener.audit-log-path", "/test/path");
        new AuditLogListener(requiredConfig, AuditLogFileWriter.getInstance());
    }

    @Test
    public void testCreateAuditLogListenerSuccessWithoutFullFilename()
    {
        createSimpleAuditLogListener(AuditLogFileWriter.getInstance());
    }

    @Test
    public void testCreateAuditLogListenerSuccessWithFullFilename()
    {
        createFullAuditLogListener(AuditLogFileWriter.getInstance());
    }

    @Test
    public void testSimpleLogQueryCompleted()
    {
        AuditLogFileWriter auditLogFileWriterMock = mock(AuditLogFileWriter.class);
        AuditLogListener auditLogListener = createSimpleAuditLogListener(auditLogFileWriterMock);

        auditLogListener.queryCompleted(testHelper.createNormalEvent());
        verify(auditLogFileWriterMock, times(1)).write(eq("/test/path/test-filename.log"), anyString());
    }

    @Test
    public void testFullLogQueryCompleted()
    {
        AuditLogFileWriter auditLogFileWriterMock = mock(AuditLogFileWriter.class);
        AuditLogListener auditLogListener = createFullAuditLogListener(auditLogFileWriterMock);

        auditLogListener.queryCompleted(testHelper.createNormalEvent());
        verify(auditLogFileWriterMock, times(1)).write(eq("/test/path_full/test-filename.log"), anyString());
        verify(auditLogFileWriterMock, times(1)).write(eq("/test/path_full/test-filename-full.log"), anyString());
    }

    @Test
    public void testFilter()
    {
        AuditLogFileWriter auditLogFileWriterMock = mock(AuditLogFileWriter.class);
        AuditLogListener auditLogListener = createFullAuditLogListenerWithFilter(auditLogFileWriterMock, "SRE_SYSTEM");

        auditLogListener.queryCompleted(testHelper.createNormalEvent());
        verify(auditLogFileWriterMock, times(1)).write(eq("/test/path_full/test-filename.log"), anyString());
        verify(auditLogFileWriterMock, times(1)).write(eq("/test/path_full/test-filename-full.log"), anyString());

        auditLogListener.queryCompleted(testHelper.createQueryWithSource(Optional.of("SRE_SYSTEM")));
        verify(auditLogFileWriterMock, times(2)).write(eq("/test/path_full/test-filename.log"), anyString());
        verify(auditLogFileWriterMock, times(1)).write(eq("/test/path_full/test-filename-full.log"), anyString());
    }

    @Test
    public void testFilterComplex()
    {
        AuditLogFileWriter auditLogFileWriterMock = mock(AuditLogFileWriter.class);
        AuditLogListener auditLogListener = createFullAuditLogListenerWithFilter(auditLogFileWriterMock, "(SRE_SYSTEM|Presto_team)");

        auditLogListener.queryCompleted(testHelper.createNormalEvent());
        verify(auditLogFileWriterMock, times(1)).write(eq("/test/path_full/test-filename.log"), anyString());
        verify(auditLogFileWriterMock, times(1)).write(eq("/test/path_full/test-filename-full.log"), anyString());

        auditLogListener.queryCompleted(testHelper.createQueryWithSource(Optional.of("SRE_SYSTEM")));
        verify(auditLogFileWriterMock, times(2)).write(eq("/test/path_full/test-filename.log"), anyString());
        verify(auditLogFileWriterMock, times(1)).write(eq("/test/path_full/test-filename-full.log"), anyString());

        auditLogListener.queryCompleted(testHelper.createQueryWithSource(Optional.of("Presto_team")));
        verify(auditLogFileWriterMock, times(3)).write(eq("/test/path_full/test-filename.log"), anyString());
        verify(auditLogFileWriterMock, times(1)).write(eq("/test/path_full/test-filename-full.log"), anyString());
    }
}
