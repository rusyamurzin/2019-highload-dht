package ru.mail.polis.dao;

import java.util.NoSuchElementException;

public class NoSuchElementLite extends NoSuchElementException {
    public NoSuchElementLite(final String s) {
        super(s);
    }

    @Override
    public Throwable fillInStackTrace() {
        synchronized (this) {
            return this;
        }
    }
}