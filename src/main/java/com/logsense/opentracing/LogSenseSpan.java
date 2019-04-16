package com.logsense.opentracing;

import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.tag.Tag;

import java.util.HashMap;
import java.util.Map;

public class LogSenseSpan implements Span {
    static final String LOG_KEY_EVENT = "event";
    static final String LOG_KEY_MESSAGE = "message";

    private final Object mutex = new Object();
    private final LogSenseTracer tracer;
    private final long startTimestampRelativeNanos;

    private LogSenseSpanContext context;
    private LogSenseSpanModel model;

    LogSenseSpan(LogSenseTracer tracer, LogSenseSpanContext context, LogSenseSpanModel model, long startTimestampRelativeNanos) {
        this.context = context;
        this.tracer = tracer;
        this.model = model;
        this.startTimestampRelativeNanos = startTimestampRelativeNanos;
    }

    @Override
    public SpanContext context() {
        return context;
    }

    @Override
    public void finish() {
        finish(nowMicros());
    }

    @Override
    public void finish(long finishTimeMicros) {
        synchronized (mutex) {
            this.model.setDurationMicros(durationMicros(finishTimeMicros));
            tracer.addSpan(this.model);
        }
    }

    @Override
    public LogSenseSpan setTag(String key, String value) {
        if (key == null || value == null) {
            return this;
        }
        synchronized (mutex) {
            this.model.setTagValue(key, value);
        }
        return this;
    }

    @Override
    public LogSenseSpan setTag(String key, boolean value) {
        if (key == null) {
            return this;
        }
        synchronized (mutex) {
            this.model.setTagValue(key, value);
        }
        return this;
    }

    @Override
    public LogSenseSpan setTag(String key, Number value) {
        if (key == null || value == null) {
            return this;
        }
        synchronized (mutex) {
            if (value instanceof Long || value instanceof Integer) {
                this.model.setTagValue(key, value.longValue());
            } else if (value instanceof Double || value instanceof Float) {
                this.model.setTagValue(key, value.doubleValue());
            } else {
                this.model.setTagValue(key, value.toString());
            }
        }
        return this;
    }

    @Override
    public <T> Span setTag(Tag<T> tag, T value) {
        if (tag != null && tag.getKey() != null && value != null) {
            if (value instanceof Number) {
                return setTag(tag.getKey(), (Number) value);
            } else if (value instanceof Boolean) {
                return setTag(tag.getKey(), (Boolean) value);
            } else {
                return setTag(tag.getKey(), value.toString());
            }
        }
        // FIXME This should clear out the key...
        return this;
    }

    @Override
    public synchronized String getBaggageItem(String key) {
        return context.getBaggageItem(key);
    }

    @Override
    public synchronized LogSenseSpan setBaggageItem(String key, String value) {
        context.addBaggageItem(key, value);
        return this;
    }

    public synchronized LogSenseSpan setOperationName(String operationName) {
        model.setOperationName(operationName);
        return this;
    }

    @SuppressWarnings("WeakerAccess")
    public void close() {
        finish();
    }

    public LogSenseTracer getTracer() {
        return tracer;
    }

    public final LogSenseSpan log(Map<String, ?> fields) {
        return log(nowMicros(), fields);
    }

    @Override
    public final LogSenseSpan log(long timestampMicros, Map<String, ?> fields) {
        for (Map.Entry<String, ?> kv : fields.entrySet()) {
            final Object inValue = kv.getValue();

            synchronized (mutex) {
                if (inValue instanceof String) {
                    this.model.setTagValue(kv.getKey(), (String)inValue);
                } else if (inValue instanceof Number) {
                    if (inValue instanceof Long || inValue instanceof Integer) {
                        this.model.setTagValue(kv.getKey(), ((Number) inValue).longValue());
                    } else if (inValue instanceof Double || inValue instanceof Float) {
                        this.model.setTagValue(kv.getKey(), ((Number) inValue).doubleValue());
                    } else {
                        this.model.setTagValue(kv.getKey(), inValue.toString());
                    }
                } else if (inValue instanceof Boolean) {
                    this.model.setTagValue(kv.getKey(), (Boolean) inValue);
                } else {
                    this.model.setTagValue(kv.getKey(), inValue.toString());
                }
            }

        }
        return this;
    }

    @Override
    public LogSenseSpan log(String message) {
        return log(nowMicros(), message, null);
    }

    @Override
    public LogSenseSpan log(long timestampMicroseconds, String message) {
        return log(timestampMicroseconds, message, null);
    }

    private LogSenseSpan log(long timestampMicroseconds, String message, /* @Nullable */ Object payload) {
        Map<String, Object> fields = new HashMap<>();
        fields.put("message", message);
        if (payload != null) {
            fields.put("payload", payload);
        }
        return log(timestampMicroseconds, fields);
    }

    private long nowMicros() {
        if (startTimestampRelativeNanos > 0) {
            // Note that startTimestampRelativeNanos will be -1 if the user
            // provided an explicit start timestamp in the SpanBuilder.
            long durationMicros = (System.nanoTime() - startTimestampRelativeNanos) / 1000;
            return model.getStartTimeStamp() + durationMicros;
        } else {
            return System.currentTimeMillis() * 1000;
        }
    }

    private long durationMicros(long finishTimeMicros) {
        return finishTimeMicros - model.getStartTimeStamp();
    }


}
