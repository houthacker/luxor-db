package dev.luxor.server.wal;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.luxor.server.io.LuxorFile;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import org.junit.jupiter.api.Test;

class OffHeapWALIndexTest {

  @Test
  void forceAllocateIndexPage() throws IOException {
    try (final LuxorFile shm =
        LuxorFile.open(
            Files.createTempFile("OffHeapWALIndexTest-", ""),
            StandardOpenOption.READ,
            StandardOpenOption.WRITE,
            StandardOpenOption.CREATE)) {
      final OffHeapWALIndex index = new OffHeapWALIndex(shm);

      for (int i = 0; i <= 4096; i++) {
        index.put(i, (long) i);
        assertEquals(i, index.findFrame(i));
      }
    }
  }
}
