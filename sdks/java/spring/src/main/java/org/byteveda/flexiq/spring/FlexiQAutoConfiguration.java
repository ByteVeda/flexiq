package org.byteveda.flexiq.spring;

import org.byteveda.flexiq.FlexiQ;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Auto-configures a single {@link FlexiQ} bean from {@link FlexiQProperties}.
 * The bean is closed with the application context. Define your own {@code FlexiQ}
 * bean to override it.
 */
@AutoConfiguration
@ConditionalOnClass(FlexiQ.class)
@EnableConfigurationProperties(FlexiQProperties.class)
public class FlexiQAutoConfiguration {

    /** Constructs the auto-configuration; Spring Boot instantiates it. */
    public FlexiQAutoConfiguration() {}

    /**
     * Builds the {@link FlexiQ} bean, applying only the properties that are set.
     *
     * @param properties the bound {@code flexiq.*} configuration
     * @return the queue handle, closed with the application context
     */
    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    public FlexiQ flexiq(FlexiQProperties properties) {
        FlexiQ.Builder builder = FlexiQ.builder();
        if (properties.getUrl() != null) {
            builder.url(properties.getUrl());
        }
        if (properties.getPoolSize() != null) {
            builder.poolSize(properties.getPoolSize());
        }
        if (properties.getNamespace() != null) {
            builder.namespace(properties.getNamespace());
        }
        return builder.open();
    }
}
