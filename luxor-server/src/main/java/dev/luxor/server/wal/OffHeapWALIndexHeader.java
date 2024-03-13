package dev.luxor.server.wal;

import static java.lang.foreign.ValueLayout.JAVA_SHORT;

import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.VarHandle;
import java.util.Objects;

/**
 * A {@link WALIndexHeader} implementation that is read from and written to off-heap memory.
 *
 * @author houthacker
 */
public class OffHeapWALIndexHeader implements WALIndexHeader {

  private static final String LAST_VALID_FRAME_NAME = "last_valid_frame";
  public static final MemoryLayout LAYOUT =
      MemoryLayout.structLayout(JAVA_SHORT.withName(LAST_VALID_FRAME_NAME));
  private static final VarHandle last_valid_frame =
      LAYOUT.varHandle(MemoryLayout.PathElement.groupElement(LAST_VALID_FRAME_NAME));

  /** The last valid frame within the WAL. */
  private short lastValidFrame;

  /**
   * Creates a new {@link OffHeapWALIndexHeader} that contains a copy of the data in the provided
   * {@link MemorySegment}.
   *
   * @param segment The segment of memory the data is stored in.
   */
  public OffHeapWALIndexHeader(final MemorySegment segment) {
    this.lastValidFrame = (short) last_valid_frame.get(segment);
  }

  /** {@inheritDoc} */
  @Override
  public short lastValidFrame() {
    return this.lastValidFrame;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final OffHeapWALIndexHeader that = (OffHeapWALIndexHeader) o;
    return lastValidFrame == that.lastValidFrame;
  }

  @Override
  public int hashCode() {
    return Objects.hash(lastValidFrame);
  }
}
