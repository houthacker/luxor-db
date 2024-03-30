package dev.luxor.server.wal;

import java.io.Serial;

/**
 * This exception is thrown when a {@link WriteAheadLog} is being read from disk and it cannot be
 * read fully or its checksums do not validate.
 *
 * @author houthacker
 */
public class CorruptWALException extends Exception {

  @Serial private static final long serialVersionUID = -775400053018694628L;

  /** Creates a new {@link CorruptWALException}. */
  public CorruptWALException() {
    super();
  }

  /**
   * Creates a new {@link CorruptWALException} with the provided {@code message}.
   *
   * @param message The (possibly detailed) description of the exception.
   */
  public CorruptWALException(final String message) {
    super(message);
  }

  /**
   * Creates a new {@link CorruptWALException} with the provided {@code message} and {@code cause}.
   *
   * @param message The (possibly detailed) description of the exception.
   * @param cause The upstream {@link Throwable} that caused this exception.
   */
  public CorruptWALException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
