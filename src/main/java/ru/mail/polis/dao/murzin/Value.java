package ru.mail.polis.dao.murzin;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public final class Value implements Comparable<Value> {
    private final long ts;
    private final ByteBuffer data;
    private static long lastTime;
    private static long additionalTime;

    /**
     * Value which hold data with timestamp.
     * @param ts timestamp
     * @param data stored data
     */
    public Value(final long ts, final ByteBuffer data) {
        assert ts >= 0;
        this.ts = ts;
        this.data = data;
    }

    public static Value of(final ByteBuffer data) {
        //long timestamp = getCurrentTimeNanos();
        //System.out.println(getReadonlyBufferValue(timestamp + " ts of value of ", data.duplicate(), Integer.MAX_VALUE));
        return new Value(getCurrentTimeNanos(), data.duplicate());
    }

    public static Value tombstone() {
        //long timestamp = getCurrentTimeNanos();
        //System.out.println("Tombstone with timestamp is " + timestamp);
        return new Value(getCurrentTimeNanos(), null);
    }

    public boolean isRemoved() {
        return data == null;
    }

    /**
     * Get data as buffer.
     * @return data as byte buffer
     */
    public ByteBuffer getData() {
        if (data == null) {
            throw new IllegalStateException("Data is null!");
        }
        return data.asReadOnlyBuffer();
    }

    @Override
    public int compareTo(@NotNull final Value o) {
        /*if (data != null && o.data != null) {
            data.equals(o.data);
        }*/
        return -Long.compare(ts, o.ts);
    }

    public long getTimeStamp() {
        return ts;
    }

    /**
     * Get current time in nanoseconds.
     * @return current time in nanoseconds
     */
    public synchronized static long getCurrentTimeNanos() {
        final long currentTime = System.currentTimeMillis();
        if (currentTime != lastTime) {
            additionalTime = 0;
            lastTime = currentTime;
        }
        return currentTime * 1_000_000 + ++additionalTime;
    }

    public static String getReadonlyBufferValue(String prefix, ByteBuffer b, int limit) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("\n").append(prefix).append(" ").append(" value is ");
        for (int j = 0; j < b.capacity() && j < limit; j++) {
            stringBuilder.append(" ").append(b.get(j));
        }
        stringBuilder.append("\n");
        return stringBuilder.toString();
    }
}