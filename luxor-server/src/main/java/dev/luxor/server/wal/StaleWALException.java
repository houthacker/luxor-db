package dev.luxor.server.wal;

import dev.luxor.server.LuxorServerRuntimeException;

/**
 * This exception is thrown to indicate a write-ahead-log is discovered, but it is a leftover from a
 * previous system failure. Since there is currently no way to recover from this (WAL recovery is
 * not implemented yet), this exception is unchecked.
 *
 * @author houthacker
 */
public class StaleWALException extends LuxorServerRuntimeException {

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
