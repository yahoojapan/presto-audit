package jp.co.yahoo.presto.audit.serializer;

import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryFailureInfo;
import com.facebook.presto.spi.eventlistener.QueryIOMetadata;
import com.facebook.presto.spi.eventlistener.QueryStatistics;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.airlift.json.ObjectMapperProvider;

public class FullLogSerializer
        implements LogSerializer
{
    private final ObjectMapper objectMapper;

    public FullLogSerializer()
    {
        // Initialize serializer and objectMapper
        SimpleModule serializerModule = new SimpleModule("presto-audit-serializer");
        serializerModule.addSerializer(QueryCompletedEvent.class, new QueryCompletedEventSerializer());
        serializerModule.addSerializer(QueryStatistics.class, new QueryStatisticsSerializer());
        serializerModule.addSerializer(QueryIOMetadata.class, new QueryIOMetadataSerializer());
        serializerModule.addSerializer(QueryFailureInfo.class, new QueryFailureInfoSerializer());
        objectMapper = new ObjectMapperProvider().get().registerModule(serializerModule);
    }

    @Override
    public String serialize(QueryCompletedEvent event) throws JsonProcessingException
    {
        return objectMapper.writeValueAsString(event);
    }
}
