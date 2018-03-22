package jp.co.yahoo.presto.audit;

import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

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
}
