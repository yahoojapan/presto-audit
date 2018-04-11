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
