package dev.luxor.server.wal;

import java.io.Serial;

/**
 * This exception is thrown whenever writing to the WAL fails, and it is impossible to recover from
 * this. This might be because of a various set of reasons, depending on the implementation of the
 * {@link WriteAheadLog}.
 *
 * @author houthacker
 */
public class WALWriteException extends RuntimeException {

  @Serial private static final long serialVersionUID = 3467452162201633347L;

  /** Creates a new {@link WALWriteException} with no detail message. */
  public WALWriteException() {
    super();
  }

  /**
   * Creates a new {@link WALWriteException} with the given {@code message}.
   *
   * @param message A message describing the exception in (some) detail.
   */
  public WALWriteException(final String message) {
    super(message);
  }

  /**
   * Creates a new {@link WALWriteException} with the given detail message and cause.
   *
   * @param message A message describing the exception in (some) detail.
   * @param cause The upstream {@link Throwable} that caused this exception.
   */
  public WALWriteException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
