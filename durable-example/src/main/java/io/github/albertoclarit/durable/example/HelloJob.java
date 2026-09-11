package io.github.albertoclarit.durable.example;

import io.github.albertoclarit.durable.ActivityOptions;
import io.github.albertoclarit.durable.Context;
import io.github.albertoclarit.durable.DurableJob;

public class HelloJob implements DurableJob<String> {

    private final String name;

    public HelloJob() {
        this("world");
    }

    public HelloJob(String name) {
        this.name = name;
    }

    @Override
    public String run(Context ctx) {
        String greeting = ctx.step("greet", () -> "hello " + name);
        return ctx.activity(
                "announce",
                ActivityOptions.idempotencyKey("hello:" + name),
                () -> greeting.toUpperCase()
        );
    }
}
