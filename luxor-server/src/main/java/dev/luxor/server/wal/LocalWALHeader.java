package dev.luxor.server.wal;

import static java.util.Objects.requireNonNull;

import dev.luxor.server.algo.FNV1a;
import dev.luxor.server.io.LuxorFile;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

/**
 * An in-memory copy of the header in a {@link LocalWAL}.
 *
 * @author houthacker
 */
public final class LocalWALHeader implements WALHeader {

  /** The byte size of a serialized {@link LocalWALHeader}. */
  public static final int BYTES = 32;

  /**
   * The header magic can be used to assess the validity of the header. Although this method is
   * quicker, it is also less precise than validating the checksum.
   */
  private final int magic;

  /** The database size in pages at the time this header was copied to on-heap memory. */
  private final long dbSize;

  /** The amount of successfully executed checkpoints. */
  private final int checkpointSequence;

  /** The random salt. */
  private final int randomSalt;

  /** The sequential salt. */
  private final int sequentialSalt;

  /** The FNV1a checksum of the first 24 bytes of the header. */
  private final long checksum;

  /**
   * Creates a new {@link LocalWALHeader} with the given values and a calculated {@code checksum}.
   *
   * @param magic The header magic.
   * @param dbSize The database size in pages.
   * @param checkpointSequence The amount of checkpoints executed.
   * @param randomSalt The random salt.
   * @param sequentialSalt The sequential salt.
   */
  private LocalWALHeader(
      final int magic,
      final long dbSize,
      final int checkpointSequence,
      final int randomSalt,
      final int sequentialSalt) {
    this.magic = magic;
    this.dbSize = dbSize;
    this.checkpointSequence = checkpointSequence;
    this.randomSalt = randomSalt;
    this.sequentialSalt = sequentialSalt;

    this.checksum =
        new FNV1a()
            .iterate(this.magic)
            .iterate(this.dbSize)
            .iterate(this.checkpointSequence)
            .iterate(this.randomSalt)
            .iterate(this.sequentialSalt)
            .state();
  }

  /**
   * Creates a new {@link LocalWALHeader} with the given values.
   *
   * @param magic The header magic.
   * @param dbSize The database size in pages.
   * @param checkpointSequence The amount of checkpoints executed.
   * @param randomSalt The random salt.
   * @param sequentialSalt The sequential salt.
   * @param checksum The (possibly invalid) checksum.
   */
  private LocalWALHeader(
      final int magic,
      final long dbSize,
      final int checkpointSequence,
      final int randomSalt,
      final int sequentialSalt,
      final long checksum) {
    this.magic = magic;
    this.dbSize = dbSize;
    this.checkpointSequence = checkpointSequence;
    this.randomSalt = randomSalt;
    this.sequentialSalt = sequentialSalt;
    this.checksum = checksum;
  }

  /**
   * Creates a new initialized {@link LocalWALHeader}.
   *
   * @return The new {@link LocalWALHeader}.
   */
  @SuppressWarnings(
      "java:S2245") // the RNG isn't used in a cryptographically sensitive context here.
  public static LocalWALHeader createDefault() {
    return new LocalWALHeader(
        MAGIC, 0L, 0, ThreadLocalRandom.current().nextInt(), ThreadLocalRandom.current().nextInt());
  }

  /**
   * Creates a new {@link LocalWALHeader} by reading it from the provided {@code wal}.
   *
   * @param wal The file containing the serialized {@link LocalWAL}.
   * @return The {@link LocalWALHeader}
   * @throws IOException If an I/O error occurs while reading the {@link LocalWALHeader}.
   * @throws CorruptWALException If the {@link LocalWALHeader} cannot be read completely, or if its
   *     checksum is incorrect.
   */
  public static LocalWALHeader createFromFile(final LuxorFile wal)
      throws IOException, CorruptWALException {
    requireNonNull(wal, "wal must be non-null.");

    final ByteBuffer data = ByteBuffer.allocate(BYTES);
    final int bytesRead = wal.read(data, 0L);
    if (bytesRead == BYTES) {
      data.rewind();

      final LocalWALHeader header =
          new LocalWALHeader(
              data.getInt(),
              data.getLong(),
              data.getInt(),
              data.getInt(),
              data.getInt(),
              data.getLong());

      // Only return if the header is valid.
      if (header.isValid()) {
        return header;
      }

      throw new CorruptWALException("Invalid WAL header checksum.");
    }

    // Corrupt WAL
    throw new CorruptWALException(
        String.format("Could only read %d/%d bytes of the WAL header.", bytesRead, BYTES));
  }

  /** {@inheritDoc} */
  @Override
  public int magic() {
    return this.magic;
  }

  /** {@inheritDoc} */
  @Override
  public long dbSize() {
    return this.dbSize;
  }

  /** {@inheritDoc} */
  @Override
  public int checkpointSequence() {
    return this.checkpointSequence;
  }

  /** {@inheritDoc} */
  @Override
  public int randomSalt() {
    return this.randomSalt;
  }

  /** {@inheritDoc} */
  @Override
  public int sequentialSalt() {
    return this.sequentialSalt;
  }

  /** {@inheritDoc} */
  @Override
  public long checksum() {
    return this.checksum;
  }

  /** {@inheritDoc} */
  @Override
  public ByteBuffer asByteBuffer() {
    return ByteBuffer.allocate(BYTES)
        .putInt(this.magic)
        .putLong(this.dbSize)
        .putInt(this.checkpointSequence)
        .putInt(this.randomSalt)
        .putInt(this.sequentialSalt)
        .putLong(this.checksum)
        .rewind();
  }
}
