package io.github.albertoclarit.durable.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.albertoclarit.durable.Durable;
import io.github.albertoclarit.durable.WorkflowWakePublisher;
import io.github.albertoclarit.durable.internal.DefaultDurable;
import io.github.albertoclarit.durable.internal.InMemoryWorkflowStore;
import io.github.albertoclarit.durable.internal.JdbcWorkflowStore;
import io.github.albertoclarit.durable.internal.JobInjector;
import io.github.albertoclarit.durable.internal.WorkflowRuntime;
import io.github.albertoclarit.durable.internal.WorkflowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.time.Clock;
import java.util.UUID;

@AutoConfiguration
@EnableConfigurationProperties(DurableProperties.class)
@ConditionalOnProperty(prefix = "durable", name = "enabled", havingValue = "true", matchIfMissing = true)
public class DurableAutoConfiguration {

    public static final String DURABLE_DATASOURCE = "durableDataSource";

    @Bean
    @ConditionalOnMissingBean
    public Clock durableClock() {
        return Clock.systemUTC();
    }

    @Bean
    @ConditionalOnMissingBean
    public JobInjector durableJobInjector(AutowireCapableBeanFactory beanFactory) {
        return new SpringJobInjector(beanFactory);
    }

    @Bean
    @ConditionalOnMissingBean
    public WorkflowWakePublisher durableWakePublisher() {
        return workflowId -> {
        };
    }

    @Bean
    @ConditionalOnMissingBean
    public WorkflowStore durableWorkflowStore(
            DurableProperties properties,
            Clock durableClock,
            @Autowired(required = false) @Qualifier("dataSource") DataSource primary
    ) {
        if (useJdbc(properties)) {
            return new JdbcWorkflowStore(durableJdbcDataSource(properties, primary), durableClock);
        }
        return new InMemoryWorkflowStore(durableClock);
    }

    private static DataSource durableJdbcDataSource(DurableProperties properties, DataSource primary) {
        DurableProperties.Datasource ds = properties.getDatasource();
        if (ds.isUsePrimary()) {
            if (primary == null) {
                throw new IllegalStateException("durable.datasource.use-primary=true but no primary DataSource exists");
            }
            return primary;
        }
        if (!StringUtils.hasText(ds.getJdbcUrl())) {
            throw new IllegalStateException("durable.datasource.jdbc-url is required for JDBC store");
        }
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(ds.getJdbcUrl());
        config.setUsername(ds.getUsername());
        config.setPassword(ds.getPassword());
        config.setMaximumPoolSize(ds.getMaximumPoolSize());
        config.setMinimumIdle(ds.getMinimumIdle());
        config.setPoolName(ds.getPoolName());
        config.setAutoCommit(true);
        return new HikariDataSource(config);
    }

    @Bean
    @ConditionalOnMissingBean
    public WorkflowRuntime durableWorkflowRuntime(
            WorkflowStore durableWorkflowStore,
            Clock durableClock,
            DurableProperties properties,
            Environment environment
    ) {
        String workerId = properties.getWorkerId();
        if (!StringUtils.hasText(workerId)) {
            String app = environment.getProperty("spring.application.name", "durable");
            workerId = app + "-" + UUID.randomUUID();
        }
        return new WorkflowRuntime(
                durableWorkflowStore,
                durableClock,
                properties.getPollInterval(),
                properties.getLease(),
                workerId
        );
    }

    @Bean
    @ConditionalOnMissingBean
    public Durable durable(
            WorkflowStore durableWorkflowStore,
            WorkflowRuntime durableWorkflowRuntime,
            Clock durableClock,
            JobInjector durableJobInjector,
            WorkflowWakePublisher durableWakePublisher
    ) {
        DefaultDurable client = new DefaultDurable(
                durableWorkflowStore,
                durableWorkflowRuntime,
                durableClock,
                durableJobInjector,
                durableWakePublisher
        );
        durableWorkflowRuntime.setExecutor(client);
        return client;
    }

    @Bean
    public DurableRuntimeLifecycle durableRuntimeLifecycle(WorkflowRuntime durableWorkflowRuntime) {
        return new DurableRuntimeLifecycle(durableWorkflowRuntime);
    }

    static boolean useJdbc(DurableProperties properties) {
        if ("jdbc".equalsIgnoreCase(properties.getStore())) {
            return true;
        }
        if ("memory".equalsIgnoreCase(properties.getStore())) {
            return false;
        }
        return properties.getDatasource().isUsePrimary()
                || StringUtils.hasText(properties.getDatasource().getJdbcUrl());
    }
}
