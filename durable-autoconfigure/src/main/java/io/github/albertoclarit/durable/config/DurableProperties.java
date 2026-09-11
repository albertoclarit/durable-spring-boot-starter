package io.github.albertoclarit.durable.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix = "durable")
public class DurableProperties {

    private boolean enabled = true;
    private String workerId;
    private Duration pollInterval = Duration.ofMillis(100);
    private Duration lease = Duration.ofSeconds(30);
    /**
     * {@code memory} (process-local) or {@code jdbc} (PostgreSQL).
     * If unset, JDBC is used when a datasource is configured, otherwise memory.
     */
    private String store;
    private final Datasource datasource = new Datasource();

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getWorkerId() {
        return workerId;
    }

    public void setWorkerId(String workerId) {
        this.workerId = workerId;
    }

    public Duration getPollInterval() {
        return pollInterval;
    }

    public void setPollInterval(Duration pollInterval) {
        this.pollInterval = pollInterval;
    }

    public Duration getLease() {
        return lease;
    }

    public void setLease(Duration lease) {
        this.lease = lease;
    }

    public String getStore() {
        return store;
    }

    public void setStore(String store) {
        this.store = store;
    }

    public Datasource getDatasource() {
        return datasource;
    }

    public static class Datasource {
        private boolean usePrimary;
        private String jdbcUrl;
        private String username;
        private String password;
        private int maximumPoolSize = 8;
        private int minimumIdle = 1;
        private String poolName = "durable";

        public boolean isUsePrimary() {
            return usePrimary;
        }

        public void setUsePrimary(boolean usePrimary) {
            this.usePrimary = usePrimary;
        }

        public String getJdbcUrl() {
            return jdbcUrl;
        }

        public void setJdbcUrl(String jdbcUrl) {
            this.jdbcUrl = jdbcUrl;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public int getMaximumPoolSize() {
            return maximumPoolSize;
        }

        public void setMaximumPoolSize(int maximumPoolSize) {
            this.maximumPoolSize = maximumPoolSize;
        }

        public int getMinimumIdle() {
            return minimumIdle;
        }

        public void setMinimumIdle(int minimumIdle) {
            this.minimumIdle = minimumIdle;
        }

        public String getPoolName() {
            return poolName;
        }

        public void setPoolName(String poolName) {
            this.poolName = poolName;
        }
    }
}
