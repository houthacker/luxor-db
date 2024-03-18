package dev.luxor.server.concurrent;

import dev.luxor.server.LuxorServerException;
import java.io.Serial;

/**
 * This exception is thrown when a lock cannot be acquired.
 *
 * @author houthacker
 */
public class LockFailedException extends LuxorServerException {

  @Serial private static final long serialVersionUID = 19127552004989615L;

  /** Creates a new {@link LockFailedException} with no detail message. */
  public LockFailedException() {
    super();
  }

  /**
   * Creates a new {@link LockFailedException} with the given {@code message}.
   *
   * @param message A message describing the exception in (some) detail.
   */
  public LockFailedException(final String message) {
    super(message);
  }

  /**
   * Creates a new {@link LockFailedException} with the provided {@code message} and {@code cause}.
   *
   * @param message The (possibly detailed) description of the exception.
   * @param cause The upstream {@link Throwable} that caused this exception.
   */
  public LockFailedException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
