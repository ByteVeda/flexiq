package org.byteveda.flexiq.spring;

import java.io.IOException;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.dashboard.DashboardServer;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Auto-starts a {@link DashboardServer} over the {@link FlexiQ} bean when
 * {@code flexiq.dashboard.enabled=true}. The server is stopped with the
 * application context. Define your own {@code DashboardServer} bean to override.
 */
@AutoConfiguration(after = FlexiQAutoConfiguration.class)
@ConditionalOnClass({FlexiQ.class, DashboardServer.class})
@ConditionalOnProperty(prefix = "flexiq.dashboard", name = "enabled", havingValue = "true")
@EnableConfigurationProperties(FlexiQProperties.class)
public class FlexiQDashboardAutoConfiguration {

    @Bean(destroyMethod = "close")
    @ConditionalOnBean(FlexiQ.class)
    @ConditionalOnMissingBean
    public DashboardServer flexiqDashboardServer(FlexiQ flexiq, FlexiQProperties properties) throws IOException {
        FlexiQProperties.Dashboard dashboard = properties.getDashboard();
        return DashboardServer.start(
                flexiq,
                dashboard.getPort(),
                dashboard.getToken(),
                dashboard.getStaticDir(),
                dashboard.isSecureCookies(),
                dashboard.isAuthEnabled());
    }
}
