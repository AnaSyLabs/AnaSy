package io.anasy.connector;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "anas.publisher")
public class EventPublisherProperties {

    private boolean enabled = true;
    private boolean logSuccess = false;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean isLogSuccess() {
        return logSuccess;
    }

    public void setLogSuccess(boolean logSuccess) {
        this.logSuccess = logSuccess;
    }
}
