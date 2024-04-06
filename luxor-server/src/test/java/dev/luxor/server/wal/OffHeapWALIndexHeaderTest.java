package dev.luxor.server.wal;

import static org.junit.jupiter.api.Assertions.*;

import dev.luxor.server.wal.local.OffHeapWALIndexHeader;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import org.junit.jupiter.api.Test;

class OffHeapWALIndexHeaderTest {

  @Test
  void testBuilderDeniesNullMemorySegment() {
    assertThrows(
        NullPointerException.class,
        () -> OffHeapWALIndexHeader.newBuilder(null),
        "Expect a new Builder to deny a null-MemorySegment.");
  }

  @Test
  void testBuilderDeniesInCompatibleMemory() {
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment memory =
          arena.allocate(
              OffHeapWALIndexHeader.LAYOUT.byteSize() - 1,
              OffHeapWALIndexHeader.LAYOUT.byteAlignment());

      assertThrows(
          IllegalArgumentException.class,
          () -> OffHeapWALIndexHeader.newBuilder(memory),
          "Expect a new Builder to deny an incompatible MemorySegment.");
    }
  }

  @Test
  void testBuilderSetsExpectedFields() {
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment memory =
          arena.allocate(
              OffHeapWALIndexHeader.LAYOUT.byteSize(),
              OffHeapWALIndexHeader.LAYOUT.byteAlignment());

      final OffHeapWALIndexHeader header =
          OffHeapWALIndexHeader.newBuilder(memory)
              .lastCommitFrame(1)
              .cursor(2)
              .randomSalt(3)
              .sequentialSalt(4)
              .dbSize(5L)
              .cumulativeChecksum(6L)
              .build();

      assertEquals(
          1,
          header.lastCommitFrame(),
          "Expect lastCommitFrame to return the value set in the Builder.");
      assertEquals(2, header.cursor(), "Expect cursor to point at frame 2");
      assertEquals(
          3, header.randomSalt(), "Expect randomSalt to return the value set in the Builder.");
      assertEquals(
          4,
          header.sequentialSalt(),
          "Expect sequentialSalt to return the value set in the Builder.");
      assertEquals(5L, header.dbSize(), "Expect dbSize to return the value set in the Builder.");
      assertEquals(
          6L,
          header.cumulativeChecksum(),
          "Expect cumulativeChecksum to return the value set in the Builder.");
      assertFalse(header.isEmpty(), "Expect the header to indicate it is not empty.");
    }
  }
}
