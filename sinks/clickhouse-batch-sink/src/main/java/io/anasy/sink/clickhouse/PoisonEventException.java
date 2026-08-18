package io.anasy.sink.clickhouse;

public final class PoisonEventException extends RuntimeException {

    public PoisonEventException(String message) {
        super(message);
    }
}
