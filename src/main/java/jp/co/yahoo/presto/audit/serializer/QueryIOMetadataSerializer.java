package jp.co.yahoo.presto.audit.serializer;

import com.facebook.presto.spi.eventlistener.QueryIOMetadata;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

public class QueryIOMetadataSerializer extends StdSerializer<QueryIOMetadata>
{
    public QueryIOMetadataSerializer()
    {
        this(null);
    }

    public QueryIOMetadataSerializer(Class<QueryIOMetadata> t)
    {
        super(t);
    }

    @Override
    public void serialize(
            QueryIOMetadata value, JsonGenerator jsonGenerator, SerializerProvider provider)
            throws IOException
    {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeObjectField("inputs", value.getInputs());
        jsonGenerator.writeObjectField("output", value.getOutput());
        jsonGenerator.writeEndObject();
    }
}
