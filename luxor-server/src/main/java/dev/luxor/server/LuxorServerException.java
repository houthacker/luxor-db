package dev.luxor.server;

/**
 * General exception class that all {@code luxor} API exceptions must extend.
 *
 * @author houthacker
 */
public class LuxorServerException extends Exception {

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
  public LuxorServerException(String message, Throwable cause) {
    super(message, cause);
  }
}
