package dev.luxor.server.wal;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Files;
import org.junit.jupiter.api.Test;

class LocalWALTest {

  @Test
  void create() throws Exception {
    try (final WriteAheadLog wal = LocalWAL.open(Files.createTempFile("LocalWALTest-", ""))) {
      assertEquals(0L, wal.header().dbSize(), "Expect an initially empty database.");
    }
  }
}
