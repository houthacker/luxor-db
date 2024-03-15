package dev.luxor.server.io;

import java.io.IOException;

/**
 * This exception is thrown in two cases.
 *
 * <ul>
 *   <li>Not all {@code Page} bytes can be read from the file;
 *   <li>Or, all {@code Page} bytes can be read but the page checksum is invalid.
 * </ul>
 *
 * @author houthacker
 */
public class CorruptPageException extends IOException {

  /** Creates a new {@link CorruptPageException} with no detail message. */
  public CorruptPageException() {
    super();
  }

  /**
   * Creates a new {@link CorruptPageException} with the given {@code message}.
   *
   * @param message A message describing the exception in (some) detail.
   */
  public CorruptPageException(final String message) {
    super(message);
  }

  /**
   * Creates a new {@link CorruptPageException} with the given detail message and cause.
   *
   * @param message A message describing the exception in (some) detail.
   * @param cause The upstream {@link Throwable} that caused this exception.
   */
  public CorruptPageException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
