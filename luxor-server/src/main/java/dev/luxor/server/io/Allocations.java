package dev.luxor.server.io;

import static java.util.Objects.requireNonNull;

import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;

/**
 * Utilities for {@code java.lang.foreign.*} classes.
 *
 * @author houthacker
 */
public final class Allocations {

  private Allocations() {
    throw new UnsupportedOperationException(
        "Allocations is a utility class; do not create instances of it.");
  }

  /**
   * Returns whether the given {@code segment} is compatible with the given {@code layout}. A {@link
   * MemorySegment} is compatible with a {@link MemoryLayout} if its size is not zero, is a multiple
   * of the layout size, and if its address is aligned to {@code layout.byteAlignment()}.
   *
   * @param segment The segment to test.
   * @param layout The layout that must 'fit' in the given segment.
   * @return {@code true} if the given {@code segment} matches, {@code false} otherwise.
   * @throws NullPointerException If either {@code segment} or {@code layout} is {@code null}.
   */
  public static boolean isCompatibleWith(final MemorySegment segment, final MemoryLayout layout) {
    return requireNonNull(segment, "segment must be non-null.").byteSize() != 0L
        && segment.byteSize() % requireNonNull(layout, "layout must be non-null.").byteSize() == 0L
        && segment.address() % layout.byteAlignment() == 0L;
  }
}
