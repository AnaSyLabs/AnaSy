package io.anasy.sink.duckdb;

import java.nio.file.Files;
import java.nio.file.Path;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class DuckDbBatchSinkApplication {

    public static void main(String[] args) throws Exception {
        Files.createDirectories(Path.of("data"));
        var url = System.getenv().getOrDefault("SPRING_DATASOURCE_URL", "jdbc:duckdb:./data/anas.duckdb");
        var file = url.replace("jdbc:duckdb:", "");
        if (!file.isBlank() && !file.startsWith(":memory:")) {
            var parent = Path.of(file).toAbsolutePath().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
        }
        SpringApplication.run(DuckDbBatchSinkApplication.class, args);
    }
}
