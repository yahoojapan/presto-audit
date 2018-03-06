package jp.co.yahoo.presto.audit.serializer;

import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

public class QueryCompletedEventSerializer extends StdSerializer<QueryCompletedEvent>
{
    public QueryCompletedEventSerializer()
    {
        this(null);
    }

    public QueryCompletedEventSerializer(Class<QueryCompletedEvent> t)
    {
        super(t);
    }

    @Override
    public void serialize(
            QueryCompletedEvent value, JsonGenerator jsonGenerator, SerializerProvider provider)
            throws IOException
    {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeObjectField("metadata", value.getMetadata());
        jsonGenerator.writeObjectField("statistics", value.getStatistics());
        jsonGenerator.writeObjectField("context", value.getContext());
        jsonGenerator.writeObjectField("ioMetadata", value.getIoMetadata());
        jsonGenerator.writeObjectField("failureInfo", value.getFailureInfo());
        jsonGenerator.writeObjectField("createTime", value.getCreateTime());
        jsonGenerator.writeObjectField("executionStartTime", value.getExecutionStartTime());
        jsonGenerator.writeObjectField("endTime", value.getEndTime());
        jsonGenerator.writeEndObject();
    }
}
