package com.logsense.opentracing;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class LogSenseSpanModel {
    private long durationMicros;
    private long startTimeStamp;
    private String operationName;
    private LogSenseSpanContext spanContext;

    private Long parentSpanId;
    private Long followSpanId;

    private Map<String,Object> tagValues = new HashMap<>();

    public Map<String, Object> getTagValues() {
        return tagValues;
    }

    public String getOperationName() {
        return operationName;
    }

    public long getDurationMicros() {
        return durationMicros;
    }

    public LogSenseSpanContext getSpanContext() {
        return spanContext;
    }

    public void setSpanContext(LogSenseSpanContext spanContext) {
        this.spanContext = spanContext;
    }

    public void setStartTimeStamp(long startTimeStamp) {
        this.startTimeStamp = startTimeStamp;
    }

    public long getStartTimeStamp() {
        return startTimeStamp;
    }

    public void setOperationName(String operationName) {
        this.operationName = operationName;
    }

    public void setParentSpanId(Long spanId) {
        this.parentSpanId = spanId;
    }

    public Long getParentSpanId() {
        return parentSpanId;
    }

    public void setFollowSpanId(Long followSpanId) {
        this.followSpanId = followSpanId;
    }

    public Long getFollowSpanId() {
        return followSpanId;
    }

    public void setDurationMicros(long durationMicros) {
        this.durationMicros = durationMicros;
    }

    public void setTagValue(String key, long value) {
        this.tagValues.put(key, value);
    }

    public void setTagValue(String key, String value) {
        this.tagValues.put(key, value);
    }

    public void setTagValue(String key, Boolean value) {
        this.tagValues.put(key, value);
    }

    public void setTagValue(String key, Double value) {
        this.tagValues.put(key, value);
    }

    public Map<String,Object> asMap() {
        HashMap<String,Object> out = new HashMap<>();

        Iterator<Map.Entry<String, String>> it = getSpanContext().baggageItems().iterator();
        while (it.hasNext()) {
            Map.Entry<String, String> entry = it.next();
            if (entry.getKey() != null && !entry.getKey().trim().isEmpty())
                out.put(entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Object> entry : getTagValues().entrySet()) {
            if (entry.getKey() != null && !entry.getKey().trim().isEmpty())
                out.put(entry.getKey(), entry.getValue());
        }

        out.put("operation_name", getOperationName());
        out.put("duration_us", getDurationMicros());
        out.put("trace_id", getSpanContext().getTraceId());
        out.put("span_id", getSpanContext().getSpanId());

        if (parentSpanId != null) {
            out.put("parent_span_id", parentSpanId);
        }
        if (followSpanId != null) {
            out.put("follow_span_id", followSpanId);
        }

        return out;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("\n=========");
        sb.append("\nOperation: " + getOperationName());
        sb.append("\nDuration:  " + getDurationMicros());
        sb.append("\nTrace ID:  " + getSpanContext().getTraceId());
        sb.append("\nSpan ID:   " + getSpanContext().getSpanId());
        for (Map.Entry<String, Object> entry : getTagValues().entrySet()) {
            sb.append("\n    " + entry.getKey()+": "+entry.getValue());
        }

        Iterator<Map.Entry<String, String>> it = getSpanContext().baggageItems().iterator();
        while (it.hasNext()) {
            Map.Entry<String, String> entry = it.next();
            sb.append("\n    " + entry.getKey()+": "+entry.getValue());
        }
        sb.append("\n");
        return sb.toString();
    }
}
