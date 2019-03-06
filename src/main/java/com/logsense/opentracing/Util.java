package com.logsense.opentracing;

import java.math.BigInteger;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class Util {
    static long generateRandomGUID() {
        return ThreadLocalRandom.current().nextLong();
    }

    static long fromHexString(String hexString) {
        return new BigInteger(hexString, 16).longValue();
    }

    static String toHexString(long l) {
        return Long.toHexString(l);
    }
}