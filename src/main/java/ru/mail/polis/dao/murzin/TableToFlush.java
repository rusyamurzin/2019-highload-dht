package ru.mail.polis.dao.murzin;

public class TableToFlush {
    private final int generation;
    private final Table table;

    public TableToFlush(int generation, Table table) {
        this.generation = generation;
        this.table = table;
    }

    public int getGeneration() {
        return generation;
    }

    public Table getTable() {
        return table;
    }
}
