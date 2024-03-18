package dev.luxor.server.wal;

import static org.junit.jupiter.api.Assertions.*;

import dev.luxor.server.concurrent.OutOfOrderLockException;
import java.nio.file.Files;
import org.junit.jupiter.api.Test;

class LocalWALTest {

  @Test
  void create() throws Exception {
    try (final WriteAheadLog wal = LocalWAL.open(Files.createTempFile("LocalWALTest-", ""))) {
      assertEquals(0L, wal.header().dbSize(), "Expect an initially empty database.");
    }
  }

  @Test
  void startReadTransaction() throws Exception {
    try (final WriteAheadLog wal = LocalWAL.open(Files.createTempFile("LocalWALTest-", ""))) {
      assertDoesNotThrow(wal::beginReadTransaction);
      assertDoesNotThrow(wal::endReadTransaction);
    }
  }

  @Test
  void startWriteTransactionWithoutReadTransaction() throws Exception {
    try (final WriteAheadLog wal = LocalWAL.open(Files.createTempFile("LocalWALTest-", ""))) {
      assertThrows(OutOfOrderLockException.class, wal::beginWriteTransaction);
    }
  }

  @Test
  void startWriteTransaction() throws Exception {
    try (final WriteAheadLog wal = LocalWAL.open(Files.createTempFile("LocalWALTest-", ""))) {
      assertDoesNotThrow(wal::beginReadTransaction);
      assertDoesNotThrow(wal::beginWriteTransaction);
    }
  }
}
