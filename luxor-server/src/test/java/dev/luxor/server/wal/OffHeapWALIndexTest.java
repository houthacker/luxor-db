package dev.luxor.server.wal;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.luxor.server.algo.FNV1a;
import dev.luxor.server.io.LuxorFile;
import dev.luxor.server.io.Page;
import dev.luxor.server.wal.local.OffHeapWALIndex;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.junit.jupiter.api.Test;

class OffHeapWALIndexTest {

  @Test
  void forceAllocateIndexPage() throws IOException {
    final Path path = Files.createTempFile("OffHeapWALIndexTest-", "-shm");
    try (final LuxorFile shm =
        LuxorFile.open(
            path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
      final OffHeapWALIndex index = OffHeapWALIndex.buildInitial(0L, 1, 2, shm);

      // Use a single WALFrame to 'fill' the index.
      final ByteBuffer page = ByteBuffer.allocate(Page.BYTES);

      for (int i = 0; i <= 4096; i++) {
        final long pageIndex = i + 1;
        final WALFrame frame =
            WALFrame.newBuilder()
                .page(page)
                .pageIndex(pageIndex)
                .randomSalt(1)
                .sequentialSalt(2)
                .commit(false)
                .calculateChecksum(new FNV1a())
                .build();
        index.notifyAppended(frame, i);
        assertEquals(i, index.findFrameIndexOf(pageIndex));
      }
    }
  }
}
