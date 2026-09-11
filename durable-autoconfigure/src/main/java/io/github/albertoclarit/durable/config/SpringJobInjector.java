package io.github.albertoclarit.durable.config;

import io.github.albertoclarit.durable.internal.JobInjector;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;

public final class SpringJobInjector implements JobInjector {

    private final AutowireCapableBeanFactory beanFactory;

    public SpringJobInjector(AutowireCapableBeanFactory beanFactory) {
        this.beanFactory = beanFactory;
    }

    @Override
    public void inject(Object job) {
        beanFactory.autowireBean(job);
        beanFactory.initializeBean(job, job.getClass().getName());
    }
}
