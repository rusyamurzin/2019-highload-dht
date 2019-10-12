package ru.mail.polis.dao.murzin;

import org.jetbrains.annotations.NotNull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

public interface Table {
    @NotNull
    Iterator<Cell> iterator(@NotNull ByteBuffer from) throws IOException;

    long sizeInBytes();
}