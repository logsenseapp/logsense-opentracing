package com.logsense.opentracing;


import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class LogSenseSpanContext implements io.opentracing.SpanContext {
    private final long traceId;
    private final long spanId;
    private final Map<String, String> baggage;

    private static Random random;

    public LogSenseSpanContext() {
        this(Util.generateRandomGUID(), Util.generateRandomGUID());
    }

    public LogSenseSpanContext(long traceId, long spanId) {
        this(traceId, spanId, null);
    }

    LogSenseSpanContext(long traceId) {
        this(traceId, Util.generateRandomGUID());
    }

    LogSenseSpanContext(long traceId, Map<String, String> baggage) {
        this(traceId, Util.generateRandomGUID(), baggage);
    }

    LogSenseSpanContext(Long traceId, Long spanId, Map<String, String> baggage) {
        if (traceId == null) {
            traceId = Util.generateRandomGUID();
        }

        if (spanId == null) {
            spanId = Util.generateRandomGUID();
        }

        if (baggage == null) {
            baggage = new HashMap<>();
        }

        this.traceId = traceId;
        this.spanId = spanId;
        this.baggage = baggage;
    }

    @SuppressWarnings("WeakerAccess")
    public long getSpanId() {
        return this.spanId;
    }

    @SuppressWarnings("WeakerAccess")
    public long getTraceId() {
        return this.traceId;
    }


    String getBaggageItem(String key) {
        return this.baggage.get(key);
    }

    @Override
    public Iterable<Map.Entry<String, String>> baggageItems() {
        return this.baggage.entrySet();
    }

    public void addBaggageItem(String key, String value) {
        this.baggage.put(key, value);
    }


}