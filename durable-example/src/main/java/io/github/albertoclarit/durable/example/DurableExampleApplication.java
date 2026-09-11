package io.github.albertoclarit.durable.example;

import io.github.albertoclarit.durable.annotation.EnableDurableWorkflow;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EnableDurableWorkflow
public class DurableExampleApplication {

    public static void main(String[] args) {
        SpringApplication.run(DurableExampleApplication.class, args);
    }
}
