package io.github.karthika04.tributary.storage;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

/** A clock tests can move forward by hand, so retention can be tested without waiting. */
final class MutableClock extends Clock {

    private volatile long millis;

    MutableClock(long startMillis) {
        this.millis = startMillis;
    }

    void advance(Duration duration) {
        millis += duration.toMillis();
    }

    @Override
    public long millis() {
        return millis;
    }

    @Override
    public Instant instant() {
        return Instant.ofEpochMilli(millis);
    }

    @Override
    public ZoneId getZone() {
        return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return this;
    }
}
