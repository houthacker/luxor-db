package dev.luxor.server.concurrent;

import java.io.Serial;

/**
 * This exception is thrown when an exclusive lock is being requested, but no shared lock is held at
 * that time.
 *
 * <p>If in that case a shared lock would be obtained automatically, there is no easy way to
 * distinguish between the case where a client held a shared lock prior to calling
 * lock(WALLOckType.EXCLUSIVE) and an 'automatic' shared lock. If then some error occurs while
 * trying to obtain the exclusive lock, this creates an additional issue because it cannot be
 * determined whether the shared lock must be released as well to return to the 'before' state.
 *
 * @author houthacker
 */
public class OutOfOrderLockException extends LockFailedException {

  @Serial private static final long serialVersionUID = -4234444593252327003L;

  /** Creates a new {@link OutOfOrderLockException} with no detail message. */
  public OutOfOrderLockException() {
    super();
  }

  /**
   * Creates a new {@link OutOfOrderLockException} with the given {@code message}.
   *
   * @param message A message describing the exception in (some) detail.
   */
  public OutOfOrderLockException(final String message) {
    super(message);
  }

  /**
   * Creates a new {@link OutOfOrderLockException} with the provided {@code message} and {@code
   * cause}.
   *
   * @param message The (possibly detailed) description of the exception.
   * @param cause The upstream {@link Throwable} that caused this exception.
   */
  public OutOfOrderLockException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
