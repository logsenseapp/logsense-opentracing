package com.logsense.opentracing;


import org.junit.Before;
import org.junit.Test;

public class TestLogsenseTracer {
    LogSenseTracer tracer;
    String token;

    @Before
    public void setUp() {
        token = "foo-bar";
        tracer = new LogSenseTracer(token);
    }

    @Test
    public void testItStops() {
        // do nothing actually

    }
}
