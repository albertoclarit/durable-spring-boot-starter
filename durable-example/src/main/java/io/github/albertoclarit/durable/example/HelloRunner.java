package io.github.albertoclarit.durable.example;

import io.github.albertoclarit.durable.Durable;
import io.github.albertoclarit.durable.WorkflowHandle;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class HelloRunner implements CommandLineRunner {

    private final Durable durable;

    public HelloRunner(Durable durable) {
        this.durable = durable;
    }

    @Override
    public void run(String... args) {
        WorkflowHandle<String> handle = durable.start("hello-1", new HelloJob("durable"));
        System.out.println(handle.result(Duration.ofSeconds(10)));
    }
}
