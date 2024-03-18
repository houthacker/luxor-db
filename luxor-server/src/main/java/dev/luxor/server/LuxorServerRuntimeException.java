package dev.luxor.server;

import java.io.Serial;

/**
 * Super class for all runtime exceptions thrown by the {@code luxor} server API.
 *
 * @author houthacker
 */
public class LuxorServerRuntimeException extends RuntimeException {

  @Serial private static final long serialVersionUID = -553674170873331570L;

  /** Creates a new {@link LuxorServerRuntimeException}. */
  public LuxorServerRuntimeException() {
    super();
  }

  /**
   * Creates a new {@link LuxorServerRuntimeException} with the provided {@code message}.
   *
   * @param message The (possibly detailed) description of the exception.
   */
  public LuxorServerRuntimeException(final String message) {
    super(message);
  }

  /**
   * Creates a new {@link LuxorServerRuntimeException} with the provided {@code message} and {@code
   * cause}.
   *
   * @param message The (possibly detailed) description of the exception.
   * @param cause The upstream {@link Throwable} that caused this exception.
   */
  public LuxorServerRuntimeException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
