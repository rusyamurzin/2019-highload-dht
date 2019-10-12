package ru.mail.polis.dao.murzin;

import com.google.common.collect.Iterators;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class MemTable implements Table {
    private final SortedMap<ByteBuffer, Value> map = new ConcurrentSkipListMap<>();
    private final AtomicLong sizeInBytes = new AtomicLong();

    @Override
    public long sizeInBytes() {
        return sizeInBytes.get();
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull final ByteBuffer from) {
        return Iterators.transform(
                map.tailMap(from).entrySet().iterator(),
                e -> new Cell(e.getKey(), e.getValue()));
    }

    void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        final Value previous = map.put(key.duplicate(), Value.of(value.duplicate()));
        if (previous == null) {
            sizeInBytes.addAndGet(key.remaining() + value.remaining() + Long.BYTES);
        } else if (previous.isRemoved()) {
            sizeInBytes.addAndGet(value.remaining());
        } else {
            sizeInBytes.addAndGet(value.remaining() - previous.getData().remaining());
        }
    }

    void remove(@NotNull final ByteBuffer key) {
        final Value previous = map.put(key.duplicate(), Value.tombstone());
        if (previous == null) {
            sizeInBytes.addAndGet(key.remaining() + Long.BYTES);
        } else if (!previous.isRemoved()) {
            sizeInBytes.addAndGet(-previous.getData().remaining());
        }
    }
}