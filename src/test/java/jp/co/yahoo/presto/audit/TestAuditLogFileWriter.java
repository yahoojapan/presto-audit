package jp.co.yahoo.presto.audit;

import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Constructor;

import static jp.co.yahoo.presto.audit.AuditLogFileWriter.WriterFactory;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Test(singleThreaded = true, threadPoolSize = 1)
public class TestAuditLogFileWriter
{
    private void pause()
    {
        try {
            Thread.sleep(6000);
        }
        catch (Exception e) {
        }
    }

    private void initTest()
    {
        pause();
        StackTraceElement[] stackTrace = Thread.currentThread()
                .getStackTrace();
        System.out.println("===========================");
        System.out.println(stackTrace[2].getMethodName());
        System.out.flush();
    }

    private AuditLogFileWriter getNewAuditLogFileWriter(WriterFactory writerFactory) throws Exception
    {
        Constructor<AuditLogFileWriter> constructor = AuditLogFileWriter.class.getDeclaredConstructor(WriterFactory.class);
        constructor.setAccessible(true);
        AuditLogFileWriter auditLogFileWriter = constructor.newInstance(writerFactory);
        auditLogFileWriter.start();
        return auditLogFileWriter;
    }

    @Test
    public void testSingleton()
    {
        AuditLogFileWriter auditLogFileWriter_1 = AuditLogFileWriter.getInstance();
        AuditLogFileWriter auditLogFileWriter_2 = AuditLogFileWriter.getInstance();
        assert(auditLogFileWriter_1==auditLogFileWriter_2);
    }

    @Test
    public void testNormalWrite()
    {
        initTest();
        final String FILE_NAME = "/tmp/file1";
        final String DATA = "{\"data\":\"value\"}";

        AuditLogFileWriter auditLogFileWriter = AuditLogFileWriter.getInstance();
        auditLogFileWriter.write(FILE_NAME, DATA);
    }

    @Test
    public void testWriteAutoCloseFile() throws Exception
    {
        initTest();
        final String FILE_NAME = "/tmp/file1";
        final String DATA = "{\"data\":\"value\"}";

        // Setup Spy FileWriter
        WriterFactory writerFactoryMock = mock(WriterFactory.class);
        final FileWriter[] spyFileWriter = new FileWriter[10];
        when(writerFactoryMock.getFileWriter(any(String .class))).thenAnswer(i -> {
            String filename = i.getArgument(0);
            assert(filename.equals(FILE_NAME));
            FileWriter fileWriter = new FileWriter(filename, true);
            spyFileWriter[0] = spy(fileWriter);
            doAnswer((Answer<String>) var1 -> {
                System.out.println("WRITING: " + filename + " -- " + var1.getArgument(0).toString().replaceAll("\n", "\\\\n"));
                return "";
            }).when(spyFileWriter[0]).write(anyString());
            return spyFileWriter[0];
        });

        // Test write
        AuditLogFileWriter auditLogFileWriter = getNewAuditLogFileWriter(writerFactoryMock);
        auditLogFileWriter.write(FILE_NAME, DATA);

        // Verify
        pause();
        verify(spyFileWriter[0]).write(DATA);
        verify(spyFileWriter[0]).close();
    }

    @Test
    public void testWriteCloseFileException() throws Exception
    {
        initTest();
        final String FILE_NAME = "/tmp/file1";
        final String DATA = "{\"data\":\"value\"}";

        // Setup Spy FileWriter
        WriterFactory writerFactoryMock = mock(WriterFactory.class);
        final FileWriter[] spyFileWriter = new FileWriter[10];
        when(writerFactoryMock.getFileWriter(any(String .class))).thenAnswer(i -> {
            String filename = i.getArgument(0);
            assert(filename.equals(FILE_NAME));
            FileWriter fileWriter = new FileWriter(filename, true);
            spyFileWriter[0] = spy(fileWriter);
            doAnswer((Answer<String>) var1 -> {
                System.out.println("WRITING: " + filename + " -- " + var1.getArgument(0).toString().replaceAll("\n", "\\\\n"));
                return "";
            }).when(spyFileWriter[0]).write(anyString());
            doThrow(new IOException("Mock close file exception")).when(spyFileWriter[0]).close();
            return spyFileWriter[0];
        });

        // Test write
        AuditLogFileWriter auditLogFileWriter = getNewAuditLogFileWriter(writerFactoryMock);
        auditLogFileWriter.write(FILE_NAME, DATA);

        // Verify
        pause();
        verify(spyFileWriter[0]).write(DATA);
    }

    @Test
    public void testOpenFileException() throws Exception
    {
        initTest();
        final String FILE_NAME = "/tmp/file1";
        final String DATA = "{\"data\":\"value\"}";

        // Setup Spy FileWriter
        WriterFactory writerFactoryMock = mock(WriterFactory.class);
        when(writerFactoryMock.getFileWriter(any(String .class))).thenAnswer(i -> {
            throw new IOException("Mock open file error exception");
        });

        // Test write
        AuditLogFileWriter auditLogFileWriter = getNewAuditLogFileWriter(writerFactoryMock);
        auditLogFileWriter.write(FILE_NAME, DATA);
    }

    @Test
    public void testWriteException() throws Exception
    {
        initTest();
        final String FILE_NAME = "/tmp/file1";
        final String DATA = "{\"data\":\"value\"}";

        // Setup Spy FileWriter
        WriterFactory writerFactoryMock = mock(WriterFactory.class);
        final FileWriter[] spyFileWriter = new FileWriter[10];
        when(writerFactoryMock.getFileWriter(any(String .class))).thenAnswer(i -> {
            String filename = i.getArgument(0);
            assert(filename.equals(FILE_NAME));
            FileWriter fileWriter = new FileWriter(filename, true);
            spyFileWriter[0] = spy(fileWriter);
            doThrow(new IOException("Mock write file exception")).when(spyFileWriter[0]).write(anyString());
            return spyFileWriter[0];
        });

        // Test write
        AuditLogFileWriter auditLogFileWriter = getNewAuditLogFileWriter(writerFactoryMock);
        auditLogFileWriter.write(FILE_NAME, DATA);
    }

    @Test
    public void testFullCapacityWrite() throws Exception
    {
        initTest();
        WriterFactory writerFactoryMock = mock(WriterFactory.class);
        final FileWriter[] spyFileWriter = new FileWriter[10];
        when(writerFactoryMock.getFileWriter(any(String .class))).thenAnswer(i -> {
            String filename = i.getArgument(0);
            FileWriter fileWriter = new FileWriter(filename, true);
            spyFileWriter[0] = spy(fileWriter);
            return spyFileWriter[0];
        });

        // Should log Queue full error
        AuditLogFileWriter auditLogFileWriter = getNewAuditLogFileWriter(writerFactoryMock);
        auditLogFileWriter.stop();
        for(int i=0; i<10005; i++) {
            auditLogFileWriter.write("/tmp/file1", "data1");
        }
    }

    @Test
    public void testMultipleWriteThenClose() throws Exception
    {
        initTest();
        final String FILE_NAME = "/tmp/file1";
        final String DATA = "{\"data\":\"value\"}";

        // Setup Spy FileWriter
        WriterFactory writerFactoryMock = mock(WriterFactory.class);
        final FileWriter[] spyFileWriter = new FileWriter[10];
        when(writerFactoryMock.getFileWriter(any(String .class))).thenAnswer(i -> {
            String filename = i.getArgument(0);
            assert(filename.equals(FILE_NAME));
            FileWriter fileWriter = new FileWriter(filename, true);
            spyFileWriter[0] = spy(fileWriter);
            doAnswer((Answer<String>) var1 -> {
                System.out.println("WRITING: " + filename + " -- " + var1.getArgument(0).toString().replaceAll("\n", "\\\\n"));
                return "";
            }).when(spyFileWriter[0]).write(anyString());
            return spyFileWriter[0];
        });

        // Test write
        AuditLogFileWriter auditLogFileWriter = getNewAuditLogFileWriter(writerFactoryMock);
        auditLogFileWriter.write(FILE_NAME, DATA);
        auditLogFileWriter.write(FILE_NAME, DATA);
        auditLogFileWriter.write(FILE_NAME, DATA);

        // Verify
        pause();
        verify(spyFileWriter[0], times(3)).write(DATA);
        verify(spyFileWriter[0], times(1)).close();
    }

    @Test
    public void testTimeoutReopenFile() throws Exception
    {
        initTest();
        final String FILE_NAME = "/tmp/file1";
        final String DATA = "{\"data\":\"value\"}";
        final String DATA2 = "{\"data2\":\"value\"}";

        // Setup Spy FileWriter
        WriterFactory writerFactoryMock = mock(WriterFactory.class);
        final FileWriter[] spyFileWriter = new FileWriter[10];
        when(writerFactoryMock.getFileWriter(any(String .class))).thenAnswer(i -> {
            String filename = i.getArgument(0);
            assert(filename.equals(FILE_NAME));
            FileWriter fileWriter = new FileWriter(filename, true);
            spyFileWriter[0] = spy(fileWriter);
            doAnswer((Answer<String>) var1 -> {
                System.out.println("WRITING: " + filename + " -- " + var1.getArgument(0).toString().replaceAll("\n", "\\\\n"));
                return "";
            }).when(spyFileWriter[0]).write(anyString());
            return spyFileWriter[0];
        });

        // Test write
        AuditLogFileWriter auditLogFileWriter = getNewAuditLogFileWriter(writerFactoryMock);
        auditLogFileWriter.write(FILE_NAME, DATA);
        auditLogFileWriter.write(FILE_NAME, DATA);
        auditLogFileWriter.write(FILE_NAME, DATA);

        // Verify
        pause();
        verify(spyFileWriter[0], times(3)).write(DATA);
        verify(spyFileWriter[0], times(1)).close();

        // Write again after timeout
        auditLogFileWriter.write(FILE_NAME, DATA2);
        auditLogFileWriter.write(FILE_NAME, DATA2);
        auditLogFileWriter.write(FILE_NAME, DATA2);
        verify(spyFileWriter[0], times(3)).write(DATA);
        verify(spyFileWriter[0], times(1)).close();
    }

    @Test
    public void testMultiFileMultiWriteThenClose() throws Exception
    {
        initTest();
        final String FILE_NAME = "/tmp/file1";
        final String DATA_A1 = "{\"dataA1\":\"value\"}";
        final String DATA_A2 = "{\"dataA2\":\"value\"}";

        final String FILE_NAME_2 = "/tmp/file2";
        final String DATA_B1 = "{\"data2B1\":\"value2\"}";
        final String DATA_B2 = "{\"data2B2\":\"value2\"}";

        // Setup Spy FileWriter
        WriterFactory writerFactoryMock = mock(WriterFactory.class);
        final FileWriter[] spyFileWriter = new FileWriter[10];
        when(writerFactoryMock.getFileWriter(eq(FILE_NAME))).thenAnswer(i -> {
            String filename = i.getArgument(0);
            assert(filename.equals(FILE_NAME));
            FileWriter fileWriter = new FileWriter(filename, true);
            spyFileWriter[0] = spy(fileWriter);
            doAnswer((Answer<String>) var1 -> {
                System.out.println("WRITING: " + filename + " -- " + var1.getArgument(0).toString().replaceAll("\n", "\\\\n"));
                return "";
            }).when(spyFileWriter[0]).write(anyString());
            return spyFileWriter[0];
        });
        when(writerFactoryMock.getFileWriter(eq(FILE_NAME_2))).thenAnswer(i -> {
            String filename = i.getArgument(0);
            assert(filename.equals(FILE_NAME_2));
            FileWriter fileWriter = new FileWriter(filename, true);
            spyFileWriter[1] = spy(fileWriter);
            doAnswer((Answer<String>) var1 -> {
                System.out.println("WRITING: " + filename + " -- " + var1.getArgument(0).toString().replaceAll("\n", "\\\\n"));
                return "";
            }).when(spyFileWriter[1]).write(anyString());
            return spyFileWriter[1];
        });

        // Test write
        AuditLogFileWriter auditLogFileWriter = getNewAuditLogFileWriter(writerFactoryMock);
        auditLogFileWriter.write(FILE_NAME, DATA_A1);
        auditLogFileWriter.write(FILE_NAME, DATA_A2);
        auditLogFileWriter.write(FILE_NAME_2, DATA_B1);
        auditLogFileWriter.write(FILE_NAME_2, DATA_B2);

        // Verify
        pause();
        verify(spyFileWriter[0], times(1)).write(DATA_A1);
        verify(spyFileWriter[0], times(1)).write(DATA_A2);
        verify(spyFileWriter[0], times(1)).close();
        verify(spyFileWriter[1], times(1)).write(DATA_B1);
        verify(spyFileWriter[1], times(1)).write(DATA_B2);
        verify(spyFileWriter[1], times(1)).close();
    }
}
