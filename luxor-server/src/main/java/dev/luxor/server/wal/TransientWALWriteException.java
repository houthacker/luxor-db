package dev.luxor.server.wal;

import java.io.Serial;

/**
 * This exception is thrown when writing to a {@link WriteAheadLog} fails, but calling code is
 * encouraged to retry the operation because the failure might be transient.
 *
 * @author houthacker
 */
public class TransientWALWriteException extends Exception {

  @Serial private static final long serialVersionUID = 2523782986894473270L;

  /** Creates a new {@link TransientWALWriteException} with no detail message. */
  public TransientWALWriteException() {
    super();
  }

  /**
   * Creates a new {@link TransientWALWriteException} with the given {@code message}.
   *
   * @param message A message describing the exception in (some) detail.
   */
  public TransientWALWriteException(final String message) {
    super(message);
  }

  /**
   * Creates a new {@link TransientWALWriteException} with the given detail message and cause.
   *
   * @param message A message describing the exception in (some) detail.
   * @param cause The upstream {@link Throwable} that caused this exception.
   */
  public TransientWALWriteException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
