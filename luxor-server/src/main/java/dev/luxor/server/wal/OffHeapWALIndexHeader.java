package dev.luxor.server.wal;

import static java.lang.foreign.ValueLayout.JAVA_SHORT;
import static java.util.Objects.requireNonNull;

import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.util.Objects;

/**
 * A {@link WALIndexHeader} implementation that is read from and written to off-heap memory.
 *
 * @author houthacker
 */
public class OffHeapWALIndexHeader implements WALIndexHeader {

  /** The name of the 'last_valid_frame' field in a wal_index_header struct. */
  private static final String LAST_VALID_FRAME_NAME = "last_valid_frame"; // NOPMD (LongVariable)

  /** The layout of a wal_index_header struct in off-heap memory. */
  public static final MemoryLayout LAYOUT =
      MemoryLayout.structLayout(JAVA_SHORT.withName(LAST_VALID_FRAME_NAME))
          .withName("wal_index_header");

  /** The last valid frame within the WAL. */
  private final short lastValidFrame;

  /**
   * Creates a new {@link OffHeapWALIndexHeader} that contains a copy of the data in the provided
   * {@link MemorySegment}.
   *
   * @param segment The segment of memory the data is stored in.
   */
  public OffHeapWALIndexHeader(final MemorySegment segment) {
    this.lastValidFrame =
        requireNonNull(segment, "segment must be non-null.")
            .get(
                JAVA_SHORT,
                LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement(LAST_VALID_FRAME_NAME)));
  }

  /** {@inheritDoc} */
  @Override
  public short lastValidFrame() {
    return this.lastValidFrame;
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final OffHeapWALIndexHeader that = (OffHeapWALIndexHeader) o;
    return lastValidFrame == that.lastValidFrame;
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return Objects.hash(lastValidFrame);
  }
}
