package dev.luxor.server.pager;

import static dev.luxor.server.shared.Ensure.ensureAtLeastZero;
import static java.util.Objects.requireNonNull;

import java.nio.ByteBuffer;

/**
 * A {@link Page} is a data object that contains a single B+Tree node. It is the smallest unit for
 * I/O operations within a B+Tree.
 *
 * @author houthacker
 */
public class Page {

  /** The size of a page in bytes. */
  public static final int BYTES = 4096;

  /** The index of the page in the database file. */
  private final long index;

  /** The page data. */
  private final ByteBuffer data;

  /**
   * Creates a new, empty {@link Page} with the given {@code index}.
   *
   * @param index The page index within the database file.
   * @throws IllegalArgumentException If {@code index < 0}.
   */
  public Page(final long index) {
    this(index, ByteBuffer.allocate(BYTES));
  }

  /**
   * Creates a new {@link Page}, using the provided {@code data}. {@code data.capacity()} must be
   * exactly {@link Page#BYTES}.
   *
   * @param index The page index within the database file.
   * @param data The page data.
   * @throws IllegalArgumentException If {@code index < 0}.
   * @throws NullPointerException If {@code data} is {@code null}.
   */
  public Page(final long index, final ByteBuffer data) {
    this.index = ensureAtLeastZero(index);
    this.data = requireNonNull(data, "data must be non-null.");
  }

  /**
   * Returns the index of this page within the database file.
   *
   * <p>Although the page index is <em>not</em> stored with the page in the B+Tree, its use is so
   * common that it <em>is</em> stored with the {@link Page} in memory.
   *
   * @return The page index.
   */
  public long index() {
    return this.index;
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
