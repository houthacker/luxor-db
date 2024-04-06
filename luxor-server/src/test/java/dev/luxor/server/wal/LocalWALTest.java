package dev.luxor.server.wal;

import static org.junit.jupiter.api.Assertions.*;

import dev.luxor.server.concurrent.OutOfOrderLockException;
import dev.luxor.server.io.NoSuchPageException;
import dev.luxor.server.wal.local.LocalWAL;
import java.nio.file.Files;
import org.junit.jupiter.api.Test;

class LocalWALTest {

  @Test
  void testCreate() throws Exception {
    try (final WriteAheadLog wal = LocalWAL.open(Files.createTempFile("LocalWALTest-", ""))) {
      assertEquals(0L, wal.header().dbSize(), "Expect an initially empty WAL.");
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
}
