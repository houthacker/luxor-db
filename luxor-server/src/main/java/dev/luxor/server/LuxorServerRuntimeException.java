package dev.luxor.server;

/**
 * Super class for all runtime exceptions thrown by the {@code luxor} server API.
 *
 * @author houthacker
 */
public class LuxorServerRuntimeException extends RuntimeException {

  public LuxorServerRuntimeException() {
    super();
  }

  public LuxorServerRuntimeException(final String message) {
    super(message);
  }

  public LuxorServerRuntimeException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
