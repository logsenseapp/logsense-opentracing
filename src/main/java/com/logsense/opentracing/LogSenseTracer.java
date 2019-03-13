package com.logsense.opentracing;

import io.opentracing.*;
import io.opentracing.propagation.Format;
import io.opentracing.propagation.TextMap;
import io.opentracing.util.ThreadLocalScopeManager;
import org.komamitsu.fluency.Fluency;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;


public class LogSenseTracer implements Tracer {
    private static final Locale english = new Locale("en", "US");
    private static final String PREFIX_TRACER_STATE = "ot-tracer-";
    static final String PREFIX_BAGGAGE = "ot-baggage-";
    static final String FIELD_NAME_TRACE_ID = PREFIX_TRACER_STATE + "traceid";
    static final String FIELD_NAME_SPAN_ID = PREFIX_TRACER_STATE + "spanid";
    static final String FIELD_NAME_SAMPLED = PREFIX_TRACER_STATE + "sampled";

    private ScopeManager scopeManager = new ThreadLocalScopeManager();

    private FluentEmitter emitter;
    private Thread emitterThread;
    private Boolean enabled = false;

    private final LogSenseConfig config;

    public LogSenseTracer() {
        config = LogSenseConfig.get();
        prepareFluentEmitter();
    }

    public LogSenseTracer(String customerToken) {
        Properties prop = new Properties();
        prop.setProperty(LogSenseConfig.CUSTOMER_TOKEN.getPropertyName(), customerToken);
        config = LogSenseConfig.get(prop);
        prepareFluentEmitter();
    }

    public LogSenseTracer(String customerToken, String host, int port) {
        Properties prop = new Properties();
        prop.setProperty(LogSenseConfig.CUSTOMER_TOKEN.getPropertyName(), customerToken);
        prop.setProperty(LogSenseConfig.HOST.getPropertyName(), host);
        prop.setProperty(LogSenseConfig.PORT.getPropertyName(), Integer.toString(port));
        config = LogSenseConfig.get(prop);
        prepareFluentEmitter();
    }

    private void prepareFluentEmitter() {
        if (config.getCustomerToken() != null && !config.getCustomerToken().isEmpty()) {
            this.enabled = true;
        } else {
            System.err.println("Disabling LogSense Tracer as no "+LogSenseConfig.CUSTOMER_TOKEN.toString() + " is provided");
            return;
        }

        emitter = new FluentEmitter(config.getCustomerToken(), config.getHost(), config.getPort());

        emitterThread = new Thread(emitter);
        emitterThread.setDaemon(true);
        emitterThread.start();
    }

    public boolean isDisabled() {
        return !enabled;
    }

    void addSpan(LogSenseSpanModel spanModel) {
        if (enabled) {
            emitter.emit(spanModel.asMap());
        }
    }


    private class FluentEmitter implements Runnable {
        private static final int SLEEP_MILLIS = 500;

        private String customer_token;
        private String host;
        private int port;

        private Fluency fluency;
        private List<Map<String,Object>> buffer;
        private boolean connected = false;

        public FluentEmitter(String token, String host, int port) {
            this.customer_token = token;
            this.host = host;
            this.port = port;
            this.buffer = new ArrayList<>();
        }

        public void emit(Map<String,Object> data) {
            HashMap<String,Object> allData = new HashMap<>(data);
            allData.put("cs_customer_token", customer_token);

            synchronized (buffer) {
                buffer.add(allData);
            }
        }

        private void connect() {
            if (connected)
                return;

            Fluency.Config config = new Fluency.Config();
            config.setSslEnabled(true);
            this.fluency = Fluency.defaultFluency(this.host, this.port, config);
            connected = true;
        }

        public void stop() {
            if (fluency != null) {
                try {
                    fluency.close();
                    connected = false;
                } catch (IOException e) {
                    // skip
                }
            }
        }

        @Override
        public void run() {
            while (!Thread.interrupted()) {
                try {
                    Thread.sleep(SLEEP_MILLIS);

                    connect();

                    List<Map<String,Object>> bufferCopy = new ArrayList<>();
                    synchronized (buffer) {
                        if (!buffer.isEmpty()) {
                            bufferCopy.addAll(buffer);
                            buffer.clear();
                        }
                    }

                    IOException lastException = null;
                    for (Map<String,Object> event : bufferCopy) {
                        try {
                            fluency.emit("ot", event);
                        } catch (IOException ioe) {
                            lastException = ioe;
                            connected = false;
                            // pass silently?
                        }
                    }

                    if (!connected && lastException != null) {
                        // Something went wrong, lets log it somehow
                        lastException.printStackTrace();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }


    @Override
    public ScopeManager scopeManager() {
        return scopeManager;
    }

    @Override
    public Span activeSpan() {
        Scope scope = scopeManager.active();
        return scope == null ? null : scope.span();
    }

    @Override
    public SpanBuilder buildSpan(String operationName) {
        return new LogSenseSpanBuilder(operationName, this);
    }

    @Override
    public <C> void inject(SpanContext spanContext, Format<C> format, C carrier) {
        if ( !(spanContext instanceof LogSenseSpanContext) ) {
            System.err.println("Unsupported SpanContext implementation: " + spanContext.getClass());
            return;
        }
        LogSenseSpanContext logSenseSpanContext = (LogSenseSpanContext) spanContext;
        if (format == Format.Builtin.TEXT_MAP || format == Format.Builtin.HTTP_HEADERS) {
            TextMap tm = (TextMap) carrier;

            tm.put(FIELD_NAME_TRACE_ID, Util.toHexString(logSenseSpanContext.getTraceId()));
            tm.put(FIELD_NAME_SPAN_ID, Util.toHexString(logSenseSpanContext.getSpanId()));
            tm.put(FIELD_NAME_SAMPLED, "true");
            for (Map.Entry<String, String> e : spanContext.baggageItems()) {
                tm.put(PREFIX_BAGGAGE + e.getKey(), e.getValue());
            }
        } else if (format == Format.Builtin.BINARY) {
        } else {
        }
    }

    private LogSenseSpanContext extract(TextMap carrier) {
        Long traceId = null;
        Long spanId = null;
        Map<String, String> baggage = new HashMap<>();

        for (Map.Entry<String, String> entry : carrier) {
            String key = entry.getKey().toLowerCase(english);

            if (FIELD_NAME_TRACE_ID.equals(key)) {
                traceId = Util.fromHexString(entry.getValue());
            }

            if (FIELD_NAME_SPAN_ID.equals(key)) {
                spanId = Util.fromHexString(entry.getValue());
            }

            if (key.startsWith(PREFIX_BAGGAGE)) {
                baggage.put(key.substring(PREFIX_BAGGAGE.length()), entry.getValue());
            }
        }

        if (traceId == null || spanId == null) {
            return null;
        }

        // Success.
        return new LogSenseSpanContext(traceId, spanId, baggage);
    }

    @Override
    public <C> SpanContext extract(Format<C> format, C carrier) {
        if (format == Format.Builtin.TEXT_MAP) {
            TextMap tm = (TextMap) carrier;
            return extract(tm);
        } else if (format == Format.Builtin.HTTP_HEADERS) {
            TextMap tm = (TextMap) carrier;
            return extract(tm);
        } else if (format == Format.Builtin.BINARY) {
        } else {
        }

        return null;
    }
}
