package dev.luxor.server.wal;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.luxor.server.io.LuxorFile;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import org.junit.jupiter.api.Test;

class OffHeapHashSetTest {

  @Test
  void testFlushAndCopy() throws IOException {
    try (final LuxorFile mmap =
        LuxorFile.open(
            Files.createTempFile("OffHeapHashSetTest-", ""),
            StandardOpenOption.READ,
            StandardOpenOption.WRITE,
            StandardOpenOption.CREATE)) {
      final OffHeapHashSet set = new OffHeapHashSet(mmap, 0L);

      for (int i = 0; i < 4096; i++) {
        set.put(i, 1337L + i);
        assertEquals(i, set.keyOf(1337L + i));
      }

      // Create a new set using the same backing file and validate its contents.
      final OffHeapHashSet copy = new OffHeapHashSet(mmap, 0L);
      assertEquals(
          set.size(),
          copy.size(),
          "Expect copy of OffHeapHashSet to have the same size as its original.");

      for (int i = 0; i < copy.size(); i++) {
        assertEquals(i, copy.keyOf(1337L + i));
      }
    }
  }
}
