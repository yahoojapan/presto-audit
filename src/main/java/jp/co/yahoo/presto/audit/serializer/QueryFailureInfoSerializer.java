package jp.co.yahoo.presto.audit.serializer;

import com.facebook.presto.spi.eventlistener.QueryFailureInfo;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

public class QueryFailureInfoSerializer extends StdSerializer<QueryFailureInfo>
{
    public QueryFailureInfoSerializer()
    {
        this(null);
    }

    public QueryFailureInfoSerializer(Class<QueryFailureInfo> t)
    {
        super(t);
    }

    @Override
    public void serialize(
            QueryFailureInfo value, JsonGenerator jsonGenerator, SerializerProvider provider)
            throws IOException
    {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeObjectField("errorCode", value.getErrorCode());
        jsonGenerator.writeObjectField("failureType", value.getFailureType());
        jsonGenerator.writeObjectField("failureMessage", value.getFailureMessage());
        jsonGenerator.writeObjectField("failureTask", value.getFailureTask());
        jsonGenerator.writeObjectField("failureHost", value.getFailureHost());
        jsonGenerator.writeObjectField("failuresJson", value.getFailuresJson());
        jsonGenerator.writeEndObject();
    }
}
