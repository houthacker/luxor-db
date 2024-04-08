package dev.luxor.server.wal.local;

import static org.junit.jupiter.api.Assertions.*;

import dev.luxor.server.algo.FNV1a;
import dev.luxor.server.io.LuxorFile;
import dev.luxor.server.io.Page;
import dev.luxor.server.wal.WALFrame;
import dev.luxor.server.wal.WALLockType;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.junit.jupiter.api.Assertions;
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

  @Test
  void testCannotBuildInitialWithNegativeDatabaseSize() throws IOException {
    final Path path = Files.createTempFile("OffHeapWALIndexTest-", "-shm");
    try (final LuxorFile shm =
        LuxorFile.open(
            path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
      assertThrows(
          IllegalArgumentException.class, () -> OffHeapWALIndex.buildInitial(-1L, 1, 2, shm));
    }
  }

  @Test
  void testCannotBuildInitialWithNullLuxorFile() {
    assertThrows(NullPointerException.class, () -> OffHeapWALIndex.buildInitial(0L, 1, 2, null));
  }

  @Test
  void testBuildInitialYieldsValidInstance() throws IOException {
    final Path path = Files.createTempFile("OffHeapWALIndexTest-", "-shm");
    try (final LuxorFile shm =
        LuxorFile.open(
            path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
      final OffHeapWALIndex index = OffHeapWALIndex.buildInitial(0L, 1, 2, shm);

      final OffHeapWALIndexHeader indexHeader = index.header();
      assertNotNull(indexHeader, "Expect non-null index header");
      assertEquals(
          -1,
          indexHeader.lastCommitFrame(),
          "Expect the last commit frame of an initial OffHeapWALIndex to be -1.");
      assertEquals(
          0L,
          indexHeader.dbSize(),
          "Expect an initial OffHeapWALIndex to report an empty database.");
      assertTrue(indexHeader.isEmpty(), "Expect an initial OffHeapWALIndex to be empty.");
      assertEquals(
          0,
          indexHeader.cursor(),
          "Expect the cursor of an initial OffHeapWALIndex to point to frame zero.");
      assertFalse(index.isStale(), "Expect an initial OffHeapWALIndex not to be stale.");
      Assertions.assertEquals(
          WALLockType.NONE,
          index.currentLock(),
          "Expect an initial OffHeapWALIndex to be unlocked.");
    }
  }
}
