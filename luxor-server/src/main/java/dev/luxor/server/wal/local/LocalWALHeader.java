package dev.luxor.server.wal.local;

import static dev.luxor.server.shared.Ensure.ensureAtLeastZero;
import static java.util.Objects.requireNonNull;

import dev.luxor.server.algo.FNV1a;
import dev.luxor.server.io.LuxorFile;
import dev.luxor.server.wal.CorruptWALException;
import dev.luxor.server.wal.WALHeader;
import java.io.IOException;
import java.nio.ByteBuffer;

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
   * Creates a new {@link Builder}.
   *
   * @return The new {@link Builder} instance.
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Creates a new {@link Builder} based on the given original.
   *
   * @param original The base WAL header.
   * @return The new {@link Builder} instance.
   */
  public static Builder newBuilder(final LocalWALHeader original) {
    return new Builder(original);
  }

  /**
   * Creates a new {@link LocalWALHeader} by reading it from the provided {@code wal}.
   *
   * @param wal The file containing the serialized {@link LocalWAL}.
   * @return The {@link LocalWALHeader}
   * @throws NullPointerException If {@code wal} is {@code null}.
   * @throws IllegalArgumentException If {@code offset} is negative.
   * @throws IOException If an I/O error occurs while reading the {@link LocalWALHeader}.
   * @throws CorruptWALException If the {@link LocalWALHeader} cannot be read completely, or if its
   *     checksum is incorrect.
   */
  public static LocalWALHeader readFromFile(final LuxorFile wal, final long offset)
      throws IOException, CorruptWALException {
    requireNonNull(wal, "wal must be non-null.");
    ensureAtLeastZero(offset);

    final ByteBuffer data = ByteBuffer.allocate(BYTES);
    final int bytesRead = wal.read(data, offset);
    if (bytesRead == BYTES) {
      data.rewind();

      final LocalWALHeader header =
          newBuilder()
              .magic(data.getInt())
              .dbSize(data.getLong())
              .checkpointSequence(data.getInt())
              .randomSalt(data.getInt())
              .sequentialSalt(data.getInt())
              .checksum(data.getLong())
              .build();

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

  /**
   * Builder for {@link LocalWALHeader} instances.
   *
   * @author houthacker
   */
  public static final class Builder {

    /**
     * The header magic can be used to assess the validity of the header. Although this method is
     * quicker, it is also less precise than validating the checksum.
     */
    private int magic;

    /** The database size in pages at the time this header was copied to on-heap memory. */
    private long dbSize;

    /** The amount of successfully executed checkpoints. */
    private int checkpointSequence;

    /** The random salt. */
    private int randomSalt;

    /** The sequential salt. */
    private int sequentialSalt;

    /** The FNV1a checksum of the first 24 bytes of the header. */
    private long checksum;

    /** Creates a new {@link Builder} instance. */
    private Builder() {
      /* Only instantiate from containing class. */
    }

    private Builder(final LocalWALHeader original) {
      this.magic = requireNonNull(original, "original must be non-null.").magic;
      this.dbSize = original.dbSize;
      this.checkpointSequence = original.checkpointSequence;
      this.randomSalt = original.randomSalt;
      this.sequentialSalt = original.sequentialSalt;
      this.checksum = original.checksum;
    }

    /**
     * Sets the WAL header magic. This must always be {@link WALHeader#MAGIC}.
     *
     * @param magic The magic.
     * @return This {@link Builder} instance.
     * @throws IllegalArgumentException If the magic value is invalid.
     */
    public Builder magic(final int magic) {
      if (magic == MAGIC) {
        this.magic = magic;

        return this;
      }

      throw new IllegalArgumentException("Invalid magic.");
    }

    /**
     * Sets the database size in pages.
     *
     * @param dbSize The database size.
     * @return This {@link Builder} instance.
     */
    public Builder dbSize(final long dbSize) {
      this.dbSize = ensureAtLeastZero(dbSize);

      return this;
    }

    /**
     * Sets the checkpoint sequence number of the header.
     *
     * @param checkpointSequence The checkpoint sequence.
     * @return This {@link Builder} instance.
     */
    public Builder checkpointSequence(final int checkpointSequence) {
      this.checkpointSequence = ensureAtLeastZero(checkpointSequence);

      return this;
    }

    /**
     * Sets the random salt value of the header.
     *
     * @param randomSalt The random salt value.
     * @return This {@link Builder} instance.
     */
    public Builder randomSalt(final int randomSalt) {
      this.randomSalt = randomSalt;

      return this;
    }

    /**
     * Sets the sequential salt value of the WAL header.
     *
     * @param sequentialSalt The sequential salt value.
     * @return This {@link Builder} instance.
     */
    public Builder sequentialSalt(final int sequentialSalt) {
      this.sequentialSalt = sequentialSalt;

      return this;
    }

    /**
     * Sets the header checksum.
     *
     * @param checksum The header checksum.
     * @return This {@link Builder} instance.
     */
    public Builder checksum(final long checksum) {
      this.checksum = checksum;

      return this;
    }

    /**
     * Calculates the header checksum based on the provided parameters.
     *
     * @return This {@link Builder} instance.
     */
    public Builder calculateChecksum() {
      this.checksum =
          new FNV1a()
              .iterate(this.magic)
              .iterate(this.dbSize)
              .iterate(this.checkpointSequence)
              .iterate(this.randomSalt)
              .iterate(this.sequentialSalt)
              .state();

      return this;
    }

    /**
     * Builds a new {@link LocalWALHeader} based on the provided parameters.
     *
     * @return The new wal header.
     * @throws CorruptWALException If the header checksum is invalid.
     */
    public LocalWALHeader build() throws CorruptWALException {
      final LocalWALHeader header =
          new LocalWALHeader(
              this.magic,
              this.dbSize,
              this.checkpointSequence,
              this.randomSalt,
              this.sequentialSalt,
              this.checksum);

      if (header.isValid()) {
        return header;
      }

      throw new CorruptWALException("Invalid WAL header checksum.");
    }
  }
}
