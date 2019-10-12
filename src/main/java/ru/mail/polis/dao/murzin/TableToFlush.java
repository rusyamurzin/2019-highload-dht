package ru.mail.polis.dao.murzin;

public class TableToFlush {
    private final int generation;
    private final Table table;

    public boolean isPoisonPill() {
        return poisonPill;
    }

    private final boolean poisonPill;

    public TableToFlush(int generation, Table table) {
        this(generation, table, false);
    }

    /**
     * Table to flush with generation.
     * @param generation generation of file table
     * @param table file table
     * @param poisonPill is to be killed flag
     */
    public TableToFlush(int generation, Table table, boolean poisonPill) {
        this.generation = generation;
        this.table = table;
        this.poisonPill = poisonPill;
    }

    public int getGeneration() {
        return generation;
    }

    public Table getTable() {
        return table;
    }
}
