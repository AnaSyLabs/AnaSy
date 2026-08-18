package io.anasy.sink.clickhouse;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ClickHouseBatchSinkApplication {

    public static void main(String[] args) {
        SpringApplication.run(ClickHouseBatchSinkApplication.class, args);
    }
}
