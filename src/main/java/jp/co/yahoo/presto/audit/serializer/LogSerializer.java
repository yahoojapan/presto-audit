package jp.co.yahoo.presto.audit.serializer;

import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;

import java.io.IOException;

public interface LogSerializer
{
    String serialize(QueryCompletedEvent event) throws IOException;
}
