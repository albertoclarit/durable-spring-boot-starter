package io.github.albertoclarit.durable.config;

import io.github.albertoclarit.durable.Context;
import io.github.albertoclarit.durable.Durable;
import io.github.albertoclarit.durable.DurableJob;
import io.github.albertoclarit.durable.annotation.EnableDurableWorkflow;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@TestPropertySource(properties = {
        "durable.store=memory",
        "spring.application.name=durable-test"
})
class DurableAutoConfigurationTest {

    @Autowired
    Durable durable;

    @Test
    void startsWorkflow() {
        String result = durable.start("hello", new SampleJob("nara")).result(Duration.ofSeconds(5));
        assertEquals("nara", result);
    }

    @SpringBootConfiguration
    @EnableDurableWorkflow
    static class App {
    }

    public static class SampleJob implements DurableJob<String> {
        private final String value;

        public SampleJob() {
            this("");
        }

        public SampleJob(String value) {
            this.value = value;
        }

        @Override
        public String run(Context ctx) {
            return ctx.step("v", () -> value);
        }
    }
}
