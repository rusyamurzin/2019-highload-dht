package ru.mail.polis.dao.murzin;

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
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.jetbrains.annotations.NotNull;

import com.google.common.collect.Iterators;

import ru.mail.polis.dao.Iters;

public class MemTablePool  implements Table {
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private volatile MemTable current;
    private NavigableMap<Integer, Table> pendingFlush;
    private BlockingQueue<TableToFlush> flushQueue;
    private int generation;
    private final long memFlushThreshold;

    public MemTablePool(final long memFlushThreshold, final int startGeneration) {
        this.memFlushThreshold = memFlushThreshold;
        this.generation = startGeneration;
        this.current = new MemTable();
        this.pendingFlush = new TreeMap<>();
        this.flushQueue = new ArrayBlockingQueue<>(2);
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull ByteBuffer from) throws IOException {
        lock.readLock().lock();
        final Collection<Iterator<Cell>> iterators;
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
        Iterator<Cell> withoutEquals = Iters.collapseEquals(merged, Cell::getKey);
        final Iterator<Cell> alive = Iterators.filter(withoutEquals, input -> input.getValue().getData() != null);
        return alive;
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

    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        current.upsert(key, value);
        enqueueFlush();
    }

    public void remove(@NotNull final ByteBuffer key) throws IOException {
        current.remove(key);
        enqueueFlush();
    }

    TableToFlush takeToFlush() throws InterruptedException {
        return flushQueue.take();
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
            lock.writeLock().lock();
            try {
                if (current.sizeInBytes() > memFlushThreshold) {
                    TableToFlush toFlush = new TableToFlush(generation, current);
                    try {
                        flushQueue.put(toFlush);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    generation++;
                    current = new MemTable();
                }
            } finally {
                lock.writeLock().unlock();
            }
        }
    }
}
