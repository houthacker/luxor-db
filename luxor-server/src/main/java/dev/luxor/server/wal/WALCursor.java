package dev.luxor.server.wal;

import static dev.luxor.server.shared.Ensure.ensureAtLeastZero;

import java.util.Objects;

/**
 * A {@link WALCursor} denotes the position of the next {@link WALFrame} to write.
 *
 * @author houthacker
 */
public class WALCursor {

  /** The position of the next {@link WALFrame} to write. */
  private int position;

  /**
   * Creates a new {@link WALCursor}, set at the given {@code position}.
   *
   * @param position The index of the next {@link WALFrame} to write.
   */
  public WALCursor(final int position) {
    this.position = ensureAtLeastZero(position);
  }

  /**
   * Returns the index of the next {@link WALFrame} to write.
   *
   * @return The cursor position.
   */
  public int position() {
    return this.position;
  }

  /**
   * Shifts the cursor to the next write position.
   *
   * @throws ArithmeticException If incrementing the position overflows {@link Short#MAX_VALUE}.
   */
  public void increment() {
    if (this.position == Integer.MAX_VALUE) {
      throw new ArithmeticException("Short overflow");
    }

    this.position++;
  }

  /**
   * Resets the cursor position so that the next {@link WALFrame} will be written at the beginning
   * of the WAL.
   */
  public void reset() {
    this.position = 0;
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

    final WALCursor walCursor = (WALCursor) o;
    return position == walCursor.position;
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return Objects.hash(position);
  }
}
