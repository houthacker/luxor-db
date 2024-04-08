package dev.luxor.server.wal.local;

import static org.junit.jupiter.api.Assertions.*;

import dev.luxor.server.concurrent.OutOfOrderLockException;
import dev.luxor.server.io.CorruptPageException;
import dev.luxor.server.io.NoSuchPageException;
import dev.luxor.server.io.Page;
import dev.luxor.server.wal.WALFrame;
import dev.luxor.server.wal.WALHeader;
import dev.luxor.server.wal.WriteAheadLog;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class LocalWALTest {

  private static final Logger log = LoggerFactory.getLogger(LocalWALTest.class);

  @Test
  void testOpenNew() throws Exception {
    try (final WriteAheadLog wal = LocalWAL.open(Files.createTempFile("LocalWALTest-", ""))) {
      assertEquals(0L, wal.header().dbSize(), "Expect an initially empty WAL.");
    }
  }

  @Test
  void testOpenExisting() throws Exception {
    final Path dbPath = Files.createTempFile("LocalWALTest-", "");

    // Open and close the WAL
    try (final WriteAheadLog wal = LocalWAL.open(dbPath)) {}

    // And then reopen it.
    try (final WriteAheadLog wal = LocalWAL.open(dbPath)) {
      Assertions.assertEquals(
          WALHeader.MAGIC,
          wal.header().magic(),
          "Expect valid WAL Header magic on reopening an existing WAL.");
    }
  }

  @Test
  void testCanStartReadTx() throws Exception {
    try (final WriteAheadLog wal = LocalWAL.open(Files.createTempFile("LocalWALTest-", ""))) {
      assertDoesNotThrow(wal::beginReadTransaction);
      assertDoesNotThrow(wal::endReadTransaction);
    }
  }

  @Test
  void testCannotStartWriteTxWithoutReadTx() throws Exception {
    try (final WriteAheadLog wal = LocalWAL.open(Files.createTempFile("LocalWALTest-", ""))) {
      assertThrows(OutOfOrderLockException.class, wal::beginWriteTransaction);
    }
  }

  @Test
  void testCanStartWriteTx() throws Exception {
    try (final WriteAheadLog wal = LocalWAL.open(Files.createTempFile("LocalWALTest-", ""))) {
      assertDoesNotThrow(wal::beginReadTransaction);
      assertDoesNotThrow(wal::beginWriteTransaction);
    }
  }

  @Test
  void testReadingNonExistingFrameThrows() throws Exception {
    try (final WriteAheadLog wal = LocalWAL.open(Files.createTempFile("LocalWALTest-", ""))) {
      assertThrows(NoSuchPageException.class, () -> wal.pageAt(0));
    }
  }

  @Test
  void testReadingNegativeFrameThrows() throws Exception {
    try (final WriteAheadLog wal = LocalWAL.open(Files.createTempFile("LocalWALTest-", ""))) {
      assertThrows(IllegalArgumentException.class, () -> wal.pageAt(-1));
    }
  }

  @Test
  void testWriteCommitPage() throws Exception {
    try (final WriteAheadLog wal = LocalWAL.open(Files.createTempFile("LocalWALTest-", ""))) {
      wal.beginReadTransaction();
      wal.beginWriteTransaction();

      try {
        final ByteBuffer content =
            ByteBuffer.allocate(Page.BYTES)
                .put(0, new byte[] {1, 3, 3, 7})
                .put(Page.BYTES - 5, new byte[] {1, 3, 3, 7});
        final Page page = new Page(1L, content);
        wal.writePage(page, true);

        final int frameIndex = wal.frameIndexOf(page.index());
        assertEquals(0, frameIndex, "Expect the first WAL page to be written at frame index 0.");

        final WALHeader header = wal.header();
        assertTrue(header.isValid(), "Expect a valid WALHeader.");
        assertEquals(1L, header.dbSize(), "Expect a database size of 1 after writing a page.");

        final ByteBuffer buf = wal.pageAt(frameIndex);
        assertEquals(content, buf, "Expect unchanged WAL page after it's been written.");

      } finally {
        wal.endWriteTransaction();
      }
    }
  }

  @Test
  void testReadCorruptPage() throws Exception {
    final Path dbPath = Files.createTempFile("LocalWALTest-", "");
    final Path walPath = Path.of(String.format("%s-wal", dbPath.toRealPath()));

    // Write a regular page to the WAL, ensure it is a commit, otherwise it can't be read from the
    // WAL.
    try (WriteAheadLog wal = LocalWAL.open(dbPath)) {
      final ByteBuffer content =
          ByteBuffer.allocate(Page.BYTES)
              .put(0, new byte[] {1, 3, 3, 7})
              .put(Page.BYTES - 5, new byte[] {1, 3, 3, 7});
      final Page page = new Page(1L, content);

      wal.beginReadTransaction();
      wal.beginWriteTransaction();
      try {
        wal.writePage(page, true);
      } finally {
        wal.endWriteTransaction();
      }
    }

    // Now open a new FileChannel and truncate the file so that the written page is corrupted.
    final long newSize = LocalWALHeader.BYTES + WALFrame.HEADER_BYTES + (Page.BYTES / 2);
    try (FileChannel channel =
        FileChannel.open(walPath, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
      log.debug("Truncating file at {}", walPath);
      channel.truncate(newSize);
    }

    // Finally, try to read the corrupted page from the WAL.
    try (WriteAheadLog wal = LocalWAL.open(dbPath)) {
      assertEquals(newSize, Files.size(walPath));
      wal.beginReadTransaction();
      assertThrows(
          CorruptPageException.class,
          () -> wal.pageAt(0),
          "Expect that reading a corrupt page from the WAL throws an exception.");
    }
  }
}
