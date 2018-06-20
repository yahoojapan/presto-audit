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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import io.airlift.log.Logger;
import jp.co.yahoo.presto.audit.serializer.SerializedLog;

import java.io.FileWriter;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class AuditLogFileWriter
        implements Runnable
{
    private static final int QUEUE_CAPACITY = 10000;
    private static final int FILE_TIMEOUT_SEC = 3;

    private static Logger log = Logger.get(AuditLogFileWriter.class);
    private static AuditLogFileWriter singleton;
    private final Thread t;

    private volatile boolean isTerminate = false;
    private final BlockingQueue<Map.Entry<String, SerializedLog>> queue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);
    private LoadingCache<String, FileWriter> fileWriters;

    @VisibleForTesting
    private AuditLogFileWriter(WriterFactory writerFactory, Logger logger)
    {
        this(writerFactory);
        log = logger;
    }

    private AuditLogFileWriter(WriterFactory writerFactory)
    {
        t = new Thread(this, "AuditLogWriter Thread");

        // Close file handler when cache timeout
        RemovalListener<String, FileWriter> removalListener = removal -> {
            FileWriter h = removal.getValue();
            try {
                log.debug("Close FileWriter: " + removal.getKey());
                h.close();
            }
            catch (Exception e) {
                log.error("Failed to close file: " + removal.getKey());
            }
        };

        // Open file handler when cache is needed
        fileWriters = CacheBuilder.newBuilder()
                .expireAfterWrite(FILE_TIMEOUT_SEC, TimeUnit.SECONDS)
                .removalListener(removalListener)
                .build(new CacheLoader<String, FileWriter>()
                {
                    public FileWriter load(String filename)
                            throws IOException
                    {
                        try {
                            log.debug("Open new FileWriter: " + filename);
                            return writerFactory.getFileWriter(filename);
                        }
                        catch (Exception e) {
                            log.error("Failed to open file: " + e.getMessage());
                            throw e;
                        }
                    }
                });
    }

    /**
     * Return the singleton instance for this class
     *
     * @return singleton instance
     */
    static synchronized AuditLogFileWriter getInstance()
    {
        if (singleton == null) {
            singleton = new AuditLogFileWriter(new WriterFactory());
            singleton.start();
        }
        return singleton;
    }

    /**
     * Start the thread for file writing
     */
    void start()
    {
        isTerminate = false;
        t.start();
    }

    /**
     * Terminate the thread for file writing
     */
    void stop()
    {
        isTerminate = true;
    }

    /**
     * Write data to a particular file indicated by path
     */
    void write(String path, SerializedLog data)
    {
        try {
            queue.add(new AbstractMap.SimpleEntry<>(path, data));
        }
        catch (IllegalStateException e) {
            log.error("Error adding error log to queue. Queue full while capacity is " + QUEUE_CAPACITY + ". Error: " + e.getMessage());
            log.error("Dropped queryID: " + data.getQueryId());
        }
        catch (Exception e) {
            log.error("Unknown error adding error log to queue. ErrorMessage: " + e.getMessage());
            log.error("Dropped queryID: " + data.getQueryId());
        }
    }

    @Override
    public void run()
    {
        while (!isTerminate) {
            Map.Entry<String, SerializedLog> record;

            // Poll record
            try {
                // + 1 second before cleanUP to ensure files are marked timeout
                record = queue.poll(FILE_TIMEOUT_SEC + 1, TimeUnit.SECONDS);
            }
            catch (InterruptedException e) {
                log.error("Unknown interruptedException." + e);
                continue;
            }

            if (record == null) {
                // Timeout from poll() -> release file handlers
                fileWriters.cleanUp();
            }
            else {
                try {
                    // New record for writing
                    FileWriter fileWriter = fileWriters.get(record.getKey());
                    fileWriter.write(record.getValue().getSerializedLog());
                    fileWriter.write(System.lineSeparator());
                }
                catch (Exception e) {
                    log.error("Error writing event log to file in run()." + e);
                    log.error("Dropped queryID: " + record.getValue().getQueryId());
                }
            }
        }
    }

    static class WriterFactory
    {
        FileWriter getFileWriter(String filename)
                throws IOException
        {
            return new FileWriter(filename, true);
        }
    }
}
