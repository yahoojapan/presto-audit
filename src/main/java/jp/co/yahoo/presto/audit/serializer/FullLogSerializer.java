package jp.co.yahoo.presto.audit.serializer;

import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryFailureInfo;
import com.facebook.presto.spi.eventlistener.QueryIOMetadata;
import com.facebook.presto.spi.eventlistener.QueryStatistics;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.airlift.json.ObjectMapperProvider;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FullLogSerializer
        implements LogSerializer
{
    private final ObjectMapper objectMapper;
    Pattern filter;

    public FullLogSerializer(Optional<String> auditLogFullFilter)
    {
        filter = auditLogFullFilter.map(Pattern::compile).orElse(null);
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

    @Override
    public boolean shouldOutput(QueryCompletedEvent event)
    {
        // Don't log if regex matches source name
        if (filter != null && event.getContext().getSource().isPresent()) {
            String source = event.getContext().getSource().get();
            Matcher matcher = filter.matcher(source);
            return !matcher.find();
        }
        return true;
    }
}
