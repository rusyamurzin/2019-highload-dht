package ru.mail.polis.dao.murzin;

import java.util.Iterator;

public class TableToFlush {
    private final int generation;
    private final Iterator<Cell> tableIterator;
    private final boolean poisonPill;

    public boolean isPoisonPill() {
        return poisonPill;
    }

    public TableToFlush(final int generation, final Iterator<Cell> tableIterator) {
        this(generation, tableIterator, false);
    }

    /**
     * Table to flush with generation.
     *
     * @param generation    generation of file table
     * @param tableIterator iterator through cells of table
     * @param poisonPill    is to be killed flag
     */
    public TableToFlush(final int generation, Iterator<Cell> tableIterator, final boolean poisonPill) {
        this.generation = generation;
        this.tableIterator = tableIterator;
        this.poisonPill = poisonPill;
    }

    public int getGeneration() {
        return generation;
    }

    public Iterator<Cell> getTableIterator() {
        return tableIterator;
    }
}
