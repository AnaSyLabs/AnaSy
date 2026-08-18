package io.anasy.sink.duckdb;

public final class PoisonEventException extends RuntimeException {

    public PoisonEventException(String message) {
        super(message);
    }
}
