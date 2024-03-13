package dev.luxor.server.wal;

import dev.luxor.server.LuxorServerRuntimeException;

/**
 * This exception is thrown when an {@link OffHeapHashSet} is grown recursively.
 *
 * @author houthacker
 */
public class RecursiveGrowException extends LuxorServerRuntimeException {

  public RecursiveGrowException() {
    super();
  }

  public RecursiveGrowException(final String message) {
    super(message);
  }

  public RecursiveGrowException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
