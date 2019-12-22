package ru.mail.polis.dao.murzin;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.jetbrains.annotations.NotNull;

import com.google.common.collect.Iterators;

import ru.mail.polis.dao.Iters;

public class MemTablePool  implements Table, Closeable {
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private volatile MemTable current;
    private final NavigableMap<Integer, Table> pendingFlush;
    private final BlockingQueue<TableToFlush> flushQueue;
    private int generation;

    public long getMemFlushThreshold() {
        return memFlushThreshold;
    }

    private final long memFlushThreshold;
    private final AtomicBoolean stop = new AtomicBoolean();

    /**
     * Queue for flushing tables.
     * @param memFlushThreshold threshold to flush table
     * @param startGeneration start value of generation
     */
    public MemTablePool(final long memFlushThreshold, final int startGeneration) {
        this.memFlushThreshold = memFlushThreshold;
        this.generation = startGeneration;
        this.current = new MemTable();
        this.pendingFlush = new TreeMap<>();
        this.flushQueue = new ArrayBlockingQueue<>(2);
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull final ByteBuffer from) throws IOException {
        final Collection<Iterator<Cell>> iterators;
        lock.readLock().lock();
        try {
            iterators = new ArrayList<>(pendingFlush.size() + 1);
            iterators.add(current.iterator(from));
            for (final Table table : pendingFlush.descendingMap().values()) {
                iterators.add(table.iterator(from));
            }
        } finally {
            lock.readLock().unlock();
        }

        final Iterator<Cell> merged = Iterators.mergeSorted(iterators, Cell.COMPARATOR);
        final Iterator<Cell> withoutEquals = Iters.collapseEquals(merged, Cell::getKey);
        return Iterators.filter(withoutEquals, input -> !input.getValue().isRemoved());
    }

    @Override
    public long sizeInBytes() {
        lock.readLock().lock();
        try {
            long sizeInBytes = current.sizeInBytes();
            for (final Map.Entry<Integer, Table> entry: pendingFlush.entrySet()) {
                sizeInBytes += entry.getValue().sizeInBytes();
            }
            return sizeInBytes;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Update or insert value by key.
     * @param key updated or inserted key
     * @param value updated or inserted value
     */
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        if (stop.get()) {
            throw new IllegalStateException("Already stopped!");
        }
        current.upsert(key.duplicate(), value.duplicate());
        enqueueFlush();
    }

    /**
     * Remove value by key.
     * @param key deleted key
     */
    public void remove(@NotNull final ByteBuffer key) {
        if (stop.get()) {
            throw new IllegalStateException("Already stopped!");
        }
        current.remove(key);
        enqueueFlush();
    }

    TableToFlush takeToFlush() throws InterruptedException {
        return flushQueue.take();
    }

    TableToFlush peekFlushQueue() {
        return flushQueue.peek();
    }

    void flushed(final int generation) {
        lock.writeLock().lock();
        try {
            pendingFlush.remove(generation);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void enqueueFlush() {
        if (current.sizeInBytes() > memFlushThreshold) {
            TableToFlush toFlush = null;
            lock.writeLock().lock();
            try {
                if (current.sizeInBytes() > memFlushThreshold) {
                    toFlush = new TableToFlush(generation, current);
                    generation++;
                    current = new MemTable();
                }
            } finally {
                lock.writeLock().unlock();
            }
            if (toFlush != null) {
                try {
                    flushQueue.put(toFlush);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (!stop.compareAndSet(false, true)) {
            return;
        }
        TableToFlush toFlush;
        lock.writeLock().lock();
        try {
            toFlush = new TableToFlush(generation, current, true);
        } finally {
            lock.writeLock().unlock();
        }
        try {
            flushQueue.put(toFlush);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public boolean start() {
        return stop.compareAndSet(true, false);
    }
}
