package org.byteveda.flexiq.spring;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import org.byteveda.flexiq.FlexiQ;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

class FlexiQAutoConfigurationTest {

    private final ApplicationContextRunner runner =
            new ApplicationContextRunner().withConfiguration(AutoConfigurations.of(FlexiQAutoConfiguration.class));

    @Test
    @Timeout(30)
    void providesFlexiQBeanFromProperties(@TempDir Path dir) {
        runner.withPropertyValues("flexiq.url=" + dir.resolve("s.db")).run(ctx -> {
            assertTrue(ctx.getStartupFailure() == null);
            assertNotNull(ctx.getBean(FlexiQ.class));
        });
    }
}
