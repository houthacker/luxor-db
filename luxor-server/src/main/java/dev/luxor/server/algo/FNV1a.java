package dev.luxor.server.algo;

import static java.util.Objects.nonNull;

import java.nio.ByteBuffer;

/**
 * FNV-1a is a fast, non-cryptographic hashing algorithm.
 *
 * @see <a href="https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function">
 *     Fowler–Noll–Vo_hash_function (Wikipedia)</a>
 * @author houthacker
 */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public final class FNV1a {

  /** The initial hash value. */
  private static final long OFFSET_BASIS = 0xcbf29ce484222325L;

  /** The value to multiply with when hashing a given message. */
  private static final long FNV_PRIME = 0x100000001b3L;

  /** The current hash. */
  private long state;

  /** Creates a new {@link FNV1a} instance that is initialized with the offset basis. */
  public FNV1a() {
    this.state = OFFSET_BASIS;
  }

  /**
   * Calculates the FNV1a hash of the given message, using its values at index {@code start} to
   * {@code length}. If the given message is {@code null}, this method will have no effect.
   *
   * @param message The message to hash.
   * @param start The start index (inclusive).
   * @param length The amount of bytes to hash.
   * @return This instance.
   * @throws IndexOutOfBoundsException If an attempt is made to read outside the buffer bounds.
   */
  public FNV1a iterate(final ByteBuffer message, final int start, final int length) {
    if (nonNull(message)) {
      for (int i = start; i < start + length; i++) {
        this.state *= FNV_PRIME;
        this.state ^= message.get(i);
      }
    }

    return this;
  }

  /**
   * Returns the current hashing state.
   *
   * @return The current state.
   */
  public long state() {
    return this.state;
  }
}
