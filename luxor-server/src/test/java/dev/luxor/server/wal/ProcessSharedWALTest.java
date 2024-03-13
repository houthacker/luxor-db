package dev.luxor.server.wal;

import java.io.IOException;
import java.nio.file.Files;
import org.junit.jupiter.api.Test;

class ProcessSharedWALTest {

  @Test
  void create() throws IOException {
    final WriteAheadLog wal = new ProcessSharedWAL(Files.createTempFile("luxor-wal-test-", ""));
    System.out.println("foo");
  }
}
