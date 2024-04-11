package dev.luxor.server.wal.local;

import static org.junit.jupiter.api.Assertions.*;

import dev.luxor.server.io.LuxorFile;
import dev.luxor.server.io.Page;
import dev.luxor.server.wal.CorruptWALException;
import dev.luxor.server.wal.WALFrame;
import dev.luxor.server.wal.WALSpliterator;
import java.lang.foreign.Arena;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.Test;

class LocalWALSpliteratorTest {

  private static int align(final int value, final int alignment) {
    return value + (value % alignment);
  }

  @Test
  void testFrameIteration() throws Exception {
    try (LocalWAL wal = LocalWAL.open(Files.createTempFile("local-wal-spliterator-test-", ""))) {

      // Fill the WAL with some data
      wal.beginReadTransaction();
      wal.beginWriteTransaction();
      final int pageCount = 1024;
      try {
        for (int i = 1; i <= pageCount; i++) {
          final ByteBuffer pg =
              ByteBuffer.allocate(Page.BYTES).put(new byte[] {1, 3, 3, 7}).rewind();
          final Page page = new Page(i, pg);
          wal.writePage(page, i == 1024);
        }
      } finally {
        wal.endWriteTransaction();
      }

      final WALSpliterator spliterator = wal.spliterator();
      assertEquals(
          pageCount,
          spliterator.getExactSizeIfKnown(),
          String.format("Expect a sized spliterator of exactly %d elements.", pageCount));

      final AtomicLong ppi = new AtomicLong(-1L);
      StreamSupport.stream(spliterator, spliterator.hasCharacteristics(WALSpliterator.CONCURRENT))
          .forEach(
              frame -> {
                assertTrue(frame.pageIndex() > ppi.get(), "Expect ordered stream of WAL frames.");
                final byte[] data = new byte[4];
                frame.page().get(0, data);
                assertArrayEquals(
                    new byte[] {1, 3, 3, 7},
                    data,
                    "Expect a frame-iteration-test page to start with a specific byte sequence.");

                ppi.set(frame.pageIndex());
              });
    }
  }

  @Test
  void testCreateSpliteratorWithCorruptWAL() throws Exception {
    final Path walPath = Files.createTempFile("local-wal-spliterator-test-", "-wal");
    try (Arena arena = Arena.ofConfined();
        LuxorFile wal =
            LuxorFile.open(
                walPath,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE)) {
      final OffHeapWALIndexHeader indexHeader =
          OffHeapWALIndexHeader.newBuilder(arena.allocate(OffHeapWALIndexHeader.LAYOUT))
              .cursor(0)
              .randomSalt(1)
              .sequentialSalt(2)
              .cumulativeChecksum(3L)
              .dbSize(0)
              .lastCommitFrame(-1)
              .build();

      assertThrows(CorruptWALException.class, () -> new LocalWALSpliterator(indexHeader, wal));
    }
  }

  @Test
  void testCalculateSpliteratorSizeFromFileSize() throws Exception {
    final Path walPath = Files.createTempFile("local-wal-spliterator-test-", "-wal");
    try (Arena arena = Arena.ofConfined();
        LuxorFile wal =
            LuxorFile.open(
                walPath,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE)) {

      // Write LocalWALHeader.BYTES + 1.5 WALFrame to the file.
      final ThreadLocalRandom rng = ThreadLocalRandom.current();

      // Create a ByteBuffer filled with random data
      final int size = align(LocalWALHeader.BYTES + WALFrame.BYTES + (WALFrame.BYTES / 2), 32);
      final ByteBuffer garbage = ByteBuffer.allocate(size);
      while (garbage.hasRemaining()) {
        final byte[] randomBytes = new byte[32];
        rng.nextBytes(randomBytes);

        garbage.put(randomBytes);
      }

      // And write it to the file.
      wal.write(garbage.rewind(), 0L);

      // Then prepare an index header with a frame count of zero.
      final OffHeapWALIndexHeader indexHeader =
          OffHeapWALIndexHeader.newBuilder(arena.allocate(OffHeapWALIndexHeader.LAYOUT))
              .cursor(0)
              .randomSalt(1)
              .sequentialSalt(2)
              .cumulativeChecksum(3L)
              .dbSize(1L)
              .lastCommitFrame(-1)
              .build();

      final LocalWALSpliterator spliterator = new LocalWALSpliterator(indexHeader, wal);
      assertEquals(
          indexHeader.dbSize(),
          spliterator.getExactSizeIfKnown(),
          "Expect an exact, known spliterator size.");
    }
  }
}
