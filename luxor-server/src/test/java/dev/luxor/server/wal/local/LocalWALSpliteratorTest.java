package dev.luxor.server.wal.local;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.luxor.server.io.Page;
import dev.luxor.server.wal.WALSpliterator;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.Test;

class LocalWALSpliteratorTest {

  @Test
  void testFrameIteration() throws Exception {
    try (final LocalWAL wal = LocalWAL.open(Files.createTempFile("LocalWALTest-", ""))) {

      // Fill the WAL with some data
      wal.beginReadTransaction();
      wal.beginWriteTransaction();
      final int pageCount = 1024;
      try {
        for (int i = 1; i <= pageCount; i++) {
          final ByteBuffer pg =
              ByteBuffer.allocate(Page.BYTES).put((byte) (i % Byte.MAX_VALUE)).rewind();
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
                ppi.set(frame.pageIndex());
              });
    }
  }
}
