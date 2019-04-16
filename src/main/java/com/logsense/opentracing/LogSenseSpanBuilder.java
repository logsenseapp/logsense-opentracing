package com.logsense.opentracing;

import io.opentracing.Scope;
import io.opentracing.Tracer;
import io.opentracing.noop.NoopSpan;
import io.opentracing.tag.Tag;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public class LogSenseSpanBuilder implements Tracer.SpanBuilder {
    private final String operationName;
    private final Map<String, String> stringTags;
    private final Map<String, Boolean> boolTags;
    private final Map<String, Number> numTags;
    private final LogSenseTracer tracer;

    private Long traceId = null;
    private Long spanId = null;

    private Map<Long,String> references = new HashMap<>();
    private LogSenseSpanContext parent;
    private long startTimestampMicros;
    private boolean ignoringActiveSpan;

    public static final String CHILD_OF = "child_of";
    public static final String FOLLOWS_FROM = "follows_from";

    private LogSenseSpanModel model = new LogSenseSpanModel();


    LogSenseSpanBuilder(String operationName, LogSenseTracer tracer) {
        this.operationName = operationName;
        this.tracer = tracer;
        stringTags = new HashMap<>();
        boolTags = new HashMap<>();
        numTags = new HashMap<>();
    }

    @Override
    public Tracer.SpanBuilder asChildOf(io.opentracing.SpanContext parent) {
        return addReference(CHILD_OF, parent);
    }

    @Override
    public Tracer.SpanBuilder asChildOf(io.opentracing.Span parent) {
        if (parent == null) {
            return this;
        }
        return asChildOf(parent.context());
    }

    @Override
    public Tracer.SpanBuilder addReference(String type, io.opentracing.SpanContext referredTo) {
        if (referredTo != null && (CHILD_OF.equals(type) || FOLLOWS_FROM.equals(type))) {
            parent = (LogSenseSpanContext) referredTo;
            references.put(parent.getSpanId(), type);
        }
        return this;
    }

    @Override
    public Tracer.SpanBuilder ignoreActiveSpan() {
        ignoringActiveSpan = true;
        return this;
    }

    public Tracer.SpanBuilder withTag(String key, String value) {
        stringTags.put(key, value);
        return this;
    }

    public Tracer.SpanBuilder withTag(String key, boolean value) {
        boolTags.put(key, value);
        return this;
    }

    public Tracer.SpanBuilder withTag(String key, Number value) {
        numTags.put(key, value);
        return this;
    }

    @Override
    public <T> Tracer.SpanBuilder withTag(Tag<T> tag, T t) {
        if (tag != null && tag.getKey() != null && t != null) {
            if (t instanceof Number) {
                numTags.put(tag.getKey(), (Number) t);
            } else if (t instanceof Boolean) {
                boolTags.put(tag.getKey(), (Boolean) t);
            } else {
                stringTags.put(tag.getKey(), t.toString());
            }
        }
        // FIXME This should clear out the key if t is null...
        return this;
    }

    public Tracer.SpanBuilder withStartTimestamp(long microseconds) {
        startTimestampMicros = microseconds;
        return this;
    }

    @Override
    public Scope startActive(boolean finishOnClose) {
        return tracer.scopeManager().activate(startManual(), finishOnClose);
    }

    @Override
    public io.opentracing.Span start() {
        return startManual();
    }

    @SuppressWarnings({"WeakerAccess", "UnusedReturnValue", "SameParameterValue"})
    public Tracer.SpanBuilder withTraceIdAndSpanId(long traceId, long spanId) {
        this.traceId = traceId;
        this.spanId = spanId;
        return this;
    }

    @SuppressWarnings("WeakerAccess")
    public Iterable<Map.Entry<String, String>> baggageItems() {
        if (parent == null) {
            return Collections.emptySet();
        } else {
            return parent.baggageItems();
        }
    }

    private LogSenseSpanContext activeSpanContext() {
        Scope handle = this.tracer.scopeManager().active();
        if (handle == null || handle.span() == null) {
            return null;
        }

        io.opentracing.SpanContext spanContext = handle.span().context();
        if(spanContext instanceof LogSenseSpanContext) {
            return (LogSenseSpanContext) spanContext;
        }

        return null;
    }

    @Override
    public io.opentracing.Span startManual() {
        if (tracer.isDisabled()) {
            return NoopSpan.INSTANCE;
        }

        long startTimestampRelativeNanos = -1;
        if (startTimestampMicros == 0) {
            startTimestampRelativeNanos = System.nanoTime();
            startTimestampMicros = System.currentTimeMillis() * 1000;
        }

        model.setOperationName(operationName);
        model.setStartTimeStamp(startTimestampMicros);

        Long traceId = this.traceId;

        if(parent == null && !ignoringActiveSpan) {
            parent = activeSpanContext();
            this.asChildOf(parent);
        }

        if (parent != null) {
            traceId = parent.getTraceId();
        }

        LogSenseSpanContext newSpanContext;
        if (traceId != null && spanId != null) {
            newSpanContext = new LogSenseSpanContext(traceId, spanId);
        } else if (traceId != null) {
            newSpanContext = new LogSenseSpanContext(traceId);
        } else {
            newSpanContext = new LogSenseSpanContext();
        }

        // Set the SpanContext of the span
        model.setSpanContext(newSpanContext);

        LogSenseSpan span = new LogSenseSpan(tracer, newSpanContext, model, startTimestampRelativeNanos);
        for (Map.Entry<String, String> pair : stringTags.entrySet()) {
            span.setTag(pair.getKey(), pair.getValue());
        }
        for (Map.Entry<String, Boolean> pair : boolTags.entrySet()) {
            span.setTag(pair.getKey(), pair.getValue());
        }
        for (Map.Entry<String, Number> pair : numTags.entrySet()) {
            span.setTag(pair.getKey(), pair.getValue());
        }

        for (Long x : references.keySet()) {
            String type = references.get(x);

            if (CHILD_OF.equals(type)) {
                model.setParentSpanId(x);
            } else if (FOLLOWS_FROM.equals(type)) {
                // Can there be more than one?
                model.setFollowSpanId(x);
            }
        }
        return span;
    }
}
