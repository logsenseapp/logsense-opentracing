package com.logsense.opentracing;

import com.google.auto.service.AutoService;
import io.opentracing.Tracer;
import io.opentracing.contrib.tracerresolver.TracerFactory;

@AutoService(TracerFactory.class)
public class LogSenseTracerFactory implements TracerFactory {
    @Override
    public Tracer getTracer() {
        return new LogSenseTracer();
    }
}
