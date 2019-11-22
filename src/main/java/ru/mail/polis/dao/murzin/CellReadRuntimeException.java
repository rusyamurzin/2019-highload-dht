package ru.mail.polis.dao.murzin;

@SuppressWarnings("serial")
public class CellReadRuntimeException extends RuntimeException {
    CellReadRuntimeException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
