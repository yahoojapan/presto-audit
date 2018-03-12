package jp.co.yahoo.presto.audit.serializer;

import com.facebook.presto.spi.eventlistener.QueryStatistics;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

public class QueryStatisticsSerializer extends StdSerializer<QueryStatistics>
{
    public QueryStatisticsSerializer()
    {
        this(null);
    }

    public QueryStatisticsSerializer(Class<QueryStatistics> t)
    {
        super(t);
    }

    @Override
    public void serialize(
            QueryStatistics value, JsonGenerator jsonGenerator, SerializerProvider provider)
            throws IOException
    {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeNumberField("cpuTime", value.getCpuTime().toMillis());
        jsonGenerator.writeNumberField("wallTime", value.getWallTime().toMillis());
        jsonGenerator.writeNumberField("queuedTime", value.getQueuedTime().toMillis());
        if (value.getAnalysisTime().isPresent()) {
            jsonGenerator.writeObjectField("analysisTime", value.getAnalysisTime().get().toMillis());
        }
        if (value.getDistributedPlanningTime().isPresent()) {
            jsonGenerator.writeObjectField("distributedPlanningTime", value.getDistributedPlanningTime().get().toMillis());
        }
        jsonGenerator.writeObjectField("peakMemoryBytes", value.getPeakMemoryBytes());
        jsonGenerator.writeObjectField("totalBytes", value.getTotalBytes());
        jsonGenerator.writeObjectField("totalRows", value.getTotalRows());
        jsonGenerator.writeObjectField("outputBytes", value.getOutputBytes());
        jsonGenerator.writeObjectField("outputRows", value.getOutputRows());
        jsonGenerator.writeObjectField("writtenBytes", value.getWrittenBytes());
        jsonGenerator.writeObjectField("writtenRows", value.getWrittenRows());
        jsonGenerator.writeObjectField("cumulativeMemory", value.getCumulativeMemory());
        jsonGenerator.writeObjectField("completedSplits", value.getCompletedSplits());
        jsonGenerator.writeObjectField("isComplete", value.isComplete());
        jsonGenerator.writeObjectField("cpuTimeDistribution", value.getCpuTimeDistribution());
        jsonGenerator.writeObjectField("operatorSummaries", value.getOperatorSummaries());
        jsonGenerator.writeEndObject();
    }
}
