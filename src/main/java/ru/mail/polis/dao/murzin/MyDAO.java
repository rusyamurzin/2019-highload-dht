package ru.mail.polis.dao.murzin;

import com.google.common.collect.Iterators;

import org.jetbrains.annotations.NotNull;

import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.Iters;
import ru.mail.polis.Record;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

public class MyDAO implements DAO {
    private static final String BASE_NAME = "_SSTable";
    private static final String SUFFIX = ".dat";
    private static final String TEMP = ".tmp";

    private final File base;
    private final ByteBuffer emptyBuffer = ByteBuffer.allocate(0);
    private final MemTablePool memTable;
    private final FlusherThread flusher;
    private List<FileTable> fileTables;

    private class FlusherThread extends Thread {
        public FlusherThread() {
            super("flusher");
        }

        @Override
        public void run() {
            boolean poisonReceived = false;
            while (!poisonReceived && !isInterrupted()) {
                TableToFlush toFlush = null;
                try {
                    toFlush = memTable.takeToFlush();
                    poisonReceived = toFlush.isPoisonPill();
                    flush(toFlush.getGeneration(), toFlush.getTable());
                    memTable.flushed(toFlush.getGeneration());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    /**
     * The Log-Structured Merge-Tree implementation DAO.
     * @param base path to working directory
     * @param flushThreshold threshold value of flush
     * @throws IOException if walk on base directory is failed or can`t create SSTable
     */
    public MyDAO(
            final File base,
            final long flushThreshold) throws IOException {
        this.base = base;
        assert flushThreshold >= 0L;
        final int[] generation = {0};
        this.fileTables = new CopyOnWriteArrayList<>();
        final List<Path>[] errorsCreateSSTable = new List[]{new ArrayList<>()};

        try (Stream<Path> files = Files.walk(base.toPath())) {
            files.filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().endsWith(BASE_NAME + SUFFIX))
                    .forEach(p -> {
                        addFileTable(errorsCreateSSTable[0], p);
                        generation[0] = Math.max(generation[0], getGenerationOf(p.getFileName().toString()));
                    });
        }
        this.memTable = new MemTablePool(flushThreshold, generation[0]);
        this.flusher = new FlusherThread();
        this.flusher.start();
        if (!errorsCreateSSTable[0].isEmpty()) {
            throw new IOException("can`t create FileTable with path : " + errorsCreateSSTable[0].get(0).toString());
        }
    }

    private void addFileTable(final List<Path> listErrors, final Path path) {
        try {
            fileTables.add(new FileTable(path.toFile()));
        } catch (IOException e) {
            listErrors.add(path);
        }
    }

    /**
     * Iterator for only alive cells.
     * @param from value of key of started position iterator
     * @return Iterator with alive cells
     * @throws IOException if fileTable.iterator(from) is failed
     */
    private Iterator<Cell> iteratorAliveCells(@NotNull final ByteBuffer from) throws IOException {
        final List<Iterator<Cell>> listIterators = new ArrayList<>();
        for (final FileTable fileTable : fileTables) {
            listIterators.add(fileTable.iterator(from));
        }

        listIterators.add(memTable.iterator(from));
        final Iterator<Cell> cells = Iters.collapseEquals(
                Iterators.mergeSorted(listIterators, Cell.COMPARATOR),
                Cell::getKey
        );

        return Iterators.filter(
                        cells,
                        cell -> !cell.getValue().isRemoved()
                );
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        final Iterator<Cell> alive = iteratorAliveCells(from);
        return Iterators.transform(
                alive,
                cell -> Record.of(cell.getKey(), cell.getValue().getData()));
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        memTable.upsert(key.duplicate(), value.duplicate());
    }

    void flush(final int generation, final Table table) throws IOException {
        final Iterator<Cell> iterator = table.iterator(ByteBuffer.allocate(0));
        if (iterator.hasNext()) {
            final File tmp = new File(base, generation + BASE_NAME + TEMP);
            FileTable.write(memTable.iterator(emptyBuffer), tmp);
            final File dest = new File(base, generation + BASE_NAME + SUFFIX);
            Files.move(tmp.toPath(), dest.toPath(), StandardCopyOption.ATOMIC_MOVE);
            fileTables.add(new FileTable(dest));
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        memTable.remove(key.duplicate());
    }

    @Override
    public void close() throws IOException {
        memTable.close();
        try {
            flusher.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private int getGenerationOf(final String name) {
        int result = -1;
        final long genLong = Long.parseLong(name.split("_")[0]);
        if (genLong > Integer.MAX_VALUE) {
            result = Integer.MAX_VALUE;
        } else if (genLong < Integer.MIN_VALUE) {
            result = Integer.MIN_VALUE;
        }
        else {
            result = (int) genLong;
        }
        return result;
    }
}