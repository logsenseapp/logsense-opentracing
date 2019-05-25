package com.logsense.opentracing;

import io.opentracing.*;
import io.opentracing.propagation.Format;
import io.opentracing.propagation.TextMap;
import io.opentracing.util.ThreadLocalScopeManager;
import org.komamitsu.fluency.EventTime;
import org.komamitsu.fluency.Fluency;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;


public class LogSenseTracer implements Tracer, Closeable {
    private static final Locale english = new Locale("en", "US");
    private static final String PREFIX_TRACER_STATE = "ot-tracer-";
    static final String PREFIX_BAGGAGE = "ot-baggage-";
    static final String FIELD_NAME_TRACE_ID = PREFIX_TRACER_STATE + "traceid";
    static final String FIELD_NAME_SPAN_ID = PREFIX_TRACER_STATE + "spanid";
    static final String FIELD_NAME_SAMPLED = PREFIX_TRACER_STATE + "sampled";


    public static final Logger logger = Logger.getLogger(LogSenseTracer.class.getName());

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
            logger.info("Enabling LogSense Tracer");
            this.enabled = true;
        } else {
            logger.info("Disabling LogSense Tracer as no "+LogSenseConfig.CUSTOMER_TOKEN.toString() + " is provided");
            return;
        }

        emitter = new FluentEmitter(config.getCustomerToken(), config.getHost(), config.getPort());

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                LogSenseTracer.this.close();
            }
        });

        emitterThread = new Thread(emitter);
        emitterThread.setDaemon(true);
        emitterThread.start();
    }

    public boolean isDisabled() {
        return !enabled;
    }

    void addSpan(LogSenseSpanModel spanModel) {
        if (enabled) {
            emitter.emit(spanModel.getStartTimeStamp(), spanModel.asMap());
        }
    }

    private static class FluentDataFacade {
        private static final String LOGSENSE_TOKEN_KEY = "cs_customer_token";

        FluentDataFacade(long timestampMicros, String customerToken, Map<String,Object> data) {
            Map<String, Object> allData = new HashMap<>(data);
            allData.put(LOGSENSE_TOKEN_KEY, customerToken);
            this.data = allData;
            this.timestampMicros = timestampMicros;
        }

        final long timestampMicros;
        final Map<String,Object> data;
    }

    private class FluentEmitter implements Runnable {
        private static final int SLEEP_MILLIS = 500;

        private String logsense_token;
        private String host;
        private int port;

        private Fluency fluency;
        private List<FluentDataFacade> buffer;
        private boolean connected = false;
        private boolean stopped = false;

        public FluentEmitter(String token, String host, int port) {
            this.logsense_token = token;
            this.host = host;
            this.port = port;
            this.buffer = new ArrayList<>();

        }

        public void emit(final long timestamp, final Map<String,Object> data) {
            if (logger.isLoggable(Level.FINER)) {
                logger.finer("Emitting data package: "+Arrays.toString(data.entrySet().toArray()));
            }

            synchronized (buffer) {
                buffer.add(new FluentDataFacade(timestamp, logsense_token, data));
            }
        }

        private void connect() {
            if (connected)
                return;

            LogSenseFluencyBuilder builder = new LogSenseFluencyBuilder();
            this.fluency = builder.build(this.host, this.port);
            logger.info("LogSense tracing connected to "+this.host+":"+this.port);
            connected = true;
        }


        public void stop() {
            logger.info("LogSense tracing emitter is being stopped");

            this.stopped = true;
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
            while (!Thread.interrupted() && !stopped) {
                try {
                    Thread.sleep(SLEEP_MILLIS);

                    connect();

                    List<FluentDataFacade> bufferCopy = new ArrayList<>();
                    synchronized (buffer) {
                        if (!buffer.isEmpty()) {
                            bufferCopy.addAll(buffer);
                            buffer.clear();
                        }
                    }

                    IOException lastException = null;
                    for (FluentDataFacade event : bufferCopy) {
                        try {
                            int timestampSeconds = (int) (event.timestampMicros/1000000L);
                            int timestampMicrosecondRemainder = (int) (event.timestampMicros%1000000);
                            EventTime time = new EventTime(timestampSeconds, timestampMicrosecondRemainder*1000);
                            fluency.emit("ot", time, event.data);
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
    public Scope activateSpan(Span span) {
        return scopeManager().activate(span);
    }

    @Override
    public void close() {
        emitter.stop();
    }

    @Override
    public SpanBuilder buildSpan(String operationName) {
        return new LogSenseSpanBuilder(operationName, this);
    }

    @Override
    public <C> void inject(SpanContext spanContext, Format<C> format, C carrier) {
        if ( !(spanContext instanceof LogSenseSpanContext) ) {
            logger.severe("Unsupported SpanContext implementation: " + spanContext.getClass());
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
