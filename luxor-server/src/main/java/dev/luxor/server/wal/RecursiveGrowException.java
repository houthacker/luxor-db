package dev.luxor.server.wal;

import dev.luxor.server.wal.local.OffHeapHashSet;
import java.io.Serial;

/**
 * This exception is thrown when an {@link OffHeapHashSet} is grown recursively.
 *
 * @author houthacker
 */
public class RecursiveGrowException extends RuntimeException {

  @Serial private static final long serialVersionUID = -7486596236803581804L;

  /** Creates a new {@link RecursiveGrowException} with no detail message. */
  public RecursiveGrowException() {
    super();
  }

  /**
   * Creates a new {@link RecursiveGrowException} with the given {@code message}.
   *
   * @param message A message describing the exception in (some) detail.
   */
  public RecursiveGrowException(final String message) {
    super(message);
  }

  /**
   * Creates a new {@link RecursiveGrowException} with the given detail message and cause.
   *
   * @param message A message describing the exception in (some) detail.
   * @param cause The upstream {@link Throwable} that caused this exception.
   */
  public RecursiveGrowException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
