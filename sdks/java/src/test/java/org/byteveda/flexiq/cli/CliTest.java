package org.byteveda.flexiq.cli;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import picocli.CommandLine;

class CliTest {

    @Test
    void rejectsNonSqliteBackendWithoutUrl() {
        Cli cli = new Cli();
        new CommandLine(cli).parseArgs("--backend", "postgres");
        CommandLine.ParameterException ex = assertThrows(CommandLine.ParameterException.class, cli::open);
        assertTrue(ex.getMessage().contains("--url is required"));
    }
}
