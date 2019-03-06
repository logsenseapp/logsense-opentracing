package com.logsense.opentracing;

import com.google.auto.service.AutoService;

import io.opentracing.Tracer;
import io.opentracing.contrib.tracerresolver.TracerResolver;

@AutoService(TracerResolver.class)
public class LogSenseTracerResolver extends TracerResolver {
    @Override
    protected Tracer resolve() {
        return new LogSenseTracerFactory().getTracer();
    }
}