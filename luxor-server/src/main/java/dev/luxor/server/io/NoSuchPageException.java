package dev.luxor.server.io;

import dev.luxor.server.LuxorServerException;
import java.io.Serial;

/**
 * An exception indicating that a page was requested, but that page doesn't exist.
 *
 * @author houthacker
 */
public class NoSuchPageException extends LuxorServerException {

  @Serial private static final long serialVersionUID = 8775546837674228871L;

  /** Creates a new {@link NoSuchPageException}. */
  public NoSuchPageException() {
    super();
  }

  /**
   * Creates a new {@link NoSuchPageException} with the given detail message.
   *
   * @param message A message describing the exception in (some) detail.
   */
  public NoSuchPageException(final String message) {
    super(message);
  }

  /**
   * Creates a new {@link NoSuchPageException} with the given detail message and cause.
   *
   * @param message A message describing the exception in (some) detail.
   * @param cause The upstream {@link Throwable} that caused this exception.
   */
  public NoSuchPageException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
