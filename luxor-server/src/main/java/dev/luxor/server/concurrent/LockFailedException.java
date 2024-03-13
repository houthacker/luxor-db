package dev.luxor.server.concurrent;

import dev.luxor.server.LuxorServerException;

/**
 * This exception is thrown when a lock cannot be acquired.
 *
 * @author houthacker
 */
public class LockFailedException extends LuxorServerException {
  public LockFailedException() {
    super();
  }

  public LockFailedException(final String message) {
    super(message);
  }

  public LockFailedException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
