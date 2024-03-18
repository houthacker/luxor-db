package dev.luxor.server;

import java.io.Serial;

/**
 * General exception class that all {@code luxor} API exceptions must extend.
 *
 * @author houthacker
 */
public class LuxorServerException extends Exception {

  @Serial private static final long serialVersionUID = 138266717869198125L;

  /** Creates a new {@link LuxorServerException}. */
  public LuxorServerException() {
    super();
  }

  /**
   * Creates a new {@link LuxorServerException} with the provided {@code message}.
   *
   * @param message The (possibly detailed) description of the exception.
   */
  public LuxorServerException(final String message) {
    super(message);
  }

  /**
   * Creates a new {@link LuxorServerException} with the provided {@code message} and {@code cause}.
   *
   * @param message The (possibly detailed) description of the exception.
   * @param cause The upstream {@link Throwable} that caused this exception.
   */
  public LuxorServerException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
