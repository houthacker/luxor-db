package dev.luxor.server.io;

import static java.util.Objects.requireNonNull;

import java.nio.ByteBuffer;

/**
 * A {@link Page} is a data object that contains a single B+Tree node. It is the smallest unit for
 * I/O operations within a B+Tree.
 *
 * @author houthacker
 */
public class Page {

  public static final int BYTES = 4096;

  /** The page data. */
  private final ByteBuffer data;

  /** Creates a new, empty {@link Page}. */
  public Page() {
    this.data = ByteBuffer.allocate(Page.BYTES);
  }

  /**
   * Creates a new {@link Page}, using the provided {@code data}. {@code data.capacity()} must be
   * exactly {@link Page#BYTES}.
   *
   * @param data The page data.
   */
  public Page(final ByteBuffer data) {
    this.data = requireNonNull(data, "data must be non-null.");
  }

  /**
   * Returns a read-only, shallow copy of the page data.
   *
   * @return The page data.
   */
  public ByteBuffer readOnlyData() {
    return this.data.asReadOnlyBuffer();
  }
}
