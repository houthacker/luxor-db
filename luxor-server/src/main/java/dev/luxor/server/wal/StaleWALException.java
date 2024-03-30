package dev.luxor.server.wal;

import java.io.Serial;

/**
 * This exception is thrown when a write transaction is initiated and changes to the WAL are
 * detected since its related read transaction started.
 *
 * <p>Additionally, this exception is thrown to indicate a write-ahead-log is discovered, but it is
 * a leftover from a previous system failure. Since there is currently no way to recover from this
 * (we don't yet have WAL recovery), this exception should be wrapped within an unchecked exception
 * in this case.
 *
 * @author houthacker
 */
public class StaleWALException extends Exception {

  @Serial private static final long serialVersionUID = 3988296113702797291L;

  /** Creates a new {@link StaleWALException} with no detail message. */
  public StaleWALException() {
    super();
  }

  /**
   * Creates a new {@link StaleWALException} with the given {@code message}.
   *
   * @param message A message describing the exception in (some) detail.
   */
  public StaleWALException(final String message) {
    super(message);
  }

  /**
   * Creates a new {@link StaleWALException} with the given detail message and cause.
   *
   * @param message A message describing the exception in (some) detail.
   * @param cause The upstream {@link Throwable} that caused this exception.
   */
  public StaleWALException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
