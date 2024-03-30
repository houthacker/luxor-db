package dev.luxor.server.wal;

import dev.luxor.server.algo.FNV1a;
import java.nio.ByteBuffer;

/**
 * The header of a {@link WriteAheadLog} contains information about the write-ahead-log that is used
 * to recover after a system failure.
 *
 * @author houthacker
 */
public interface WALHeader {

  /** The WAL magic number ({@code 'LUX1'}). */
  int MAGIC = 0x4c555831;

  /**
   * The magic number used to validate the containing {@link WriteAheadLog}.
   *
   * @return The WAL magic.
   */
  int magic();

  /**
   * The database size in pages. Note that page {@code zero} is reserved for the database header.
   *
   * @return The size of the database in pages, or {@code 0} if the size is unknown.
   */
  long dbSize();

  /**
   * The amount of successfully executed checkpoints.
   *
   * @return The checkpoint count.
   */
  int checkpointSequence();

  /**
   * A random salt that is recalculated on each checkpoint.
   *
   * @return The random checkpoint salt.
   */
  int randomSalt();

  /**
   * A salt that is initialized with a random value and incremented on each checkpoint.
   *
   * @return The incremental checkpoint salt.
   */
  int sequentialSalt();

  /**
   * The FNV1a-hash of the preceding header bytes.
   *
   * @return The header hash.
   */
  long checksum();

  /**
   * Serializes this {@link WALHeader} into a new {@link ByteBuffer}.
   *
   * @return The serialized {@link WALHeader}.
   */
  ByteBuffer asByteBuffer();

  /**
   * Calculates the FNV1a checksum of this header and compares it to the stored checksum.
   *
   * @return Whether this header is valid.
   */
  default boolean isValid() {
    return this.checksum()
        == new FNV1a()
            .iterate(this.magic())
            .iterate(this.dbSize())
            .iterate(this.checkpointSequence())
            .iterate(this.randomSalt())
            .iterate(this.sequentialSalt())
            .state();
  }
}
