package dev.luxor.server.wal.local;

import static dev.luxor.server.io.Allocations.isCompatibleWith;
import static dev.luxor.server.shared.Ensure.ensureAtLeastZero;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static java.util.Objects.requireNonNull;

import dev.luxor.server.wal.WALCursor;
import dev.luxor.server.wal.WALFrame;
import dev.luxor.server.wal.WALIndexHeader;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.util.Objects;

/**
 * A {@link WALIndexHeader} implementation that is read from off-heap memory.
 *
 * @author houthacker
 */
public final class OffHeapWALIndexHeader implements WALIndexHeader {

  /** The name of the 'last_valid_frame' field in a wal_index_header struct. */
  private static final String LAST_COMMIT_FRAME_NAME = "last_commit_frame"; // NOPMD (LongVariable)

  /** The name of the 'frame_cursor' field in a wal_index_header struct. */
  private static final String WAL_CURSOR_NAME = "wal_cursor";

  /** The name of the 'random_salt' field in the index header struct. */
  private static final String RANDOM_SALT_NAME = "random_salt";

  /** The name of the 'seq_salt' field in the index header struct. */
  private static final String SEQ_SALT_NAME = "seq_salt";

  /** The name of the 'db_size' field in the index header struct. */
  private static final String DB_SIZE_NAME = "db_size";

  /** The name of the 'checksum' field in the index header struct. */
  private static final String CHECKSUM_NAME = "checksum";

  /** The layout of a wal_index_header struct in off-heap memory. */
  public static final MemoryLayout LAYOUT =
      MemoryLayout.structLayout(
              JAVA_INT.withName(LAST_COMMIT_FRAME_NAME),
              JAVA_INT.withName(WAL_CURSOR_NAME),
              JAVA_INT.withName(RANDOM_SALT_NAME),
              JAVA_INT.withName(SEQ_SALT_NAME),
              JAVA_LONG.withName(DB_SIZE_NAME),
              JAVA_LONG.withName(CHECKSUM_NAME))
          .withName("wal_index_header");

  static {
    assert LAYOUT.byteAlignment() == 8;
    assert LAYOUT.byteSize() == 32;
  }

  /** The offset in bytes of the 'last_commit_frame' field in the index header struct. */
  @SuppressWarnings("PMD.LongVariable")
  private static final long LAST_COMMIT_FRAME_OFFSET =
      LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement(LAST_COMMIT_FRAME_NAME));

  /** The offset in bytes of the 'wal_cursor' field in the index header struct. */
  private static final long WAL_CURSOR_OFFSET =
      LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement(WAL_CURSOR_NAME));

  /** The offset in bytes of the 'random_salt' field in the index header struct. */
  private static final long RANDOM_SALT_OFFSET =
      LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement(RANDOM_SALT_NAME));

  /** The offset in bytes of the 'seq_salt' field in the index header struct. */
  private static final long SEQ_SALT_OFFSET =
      LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement(SEQ_SALT_NAME));

  /** The offset in bytes of the 'db_size' field in the index header struct. */
  private static final long DB_SIZE_OFFSET =
      LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement(DB_SIZE_NAME));

  /** The offset in bytes of the 'checksum' field in the index header struct. */
  private static final long CHECKSUM_OFFSET =
      LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement(CHECKSUM_NAME));

  /** The last valid commit frame within the WAL. */
  private int lastCommitFrame;

  /** Manages the index of the next frame to write. */
  private final WALCursor cursor;

  /** The random salt. */
  @SuppressWarnings("PMD.ImmutableField")
  private int randomSalt;

  /** The sequential salt. */
  @SuppressWarnings("PMD.ImmutableField")
  private int sequentialSalt;

  /** The database size in pages. */
  private long dbSize;

  /** The cumulative checksum. */
  private long cumulativeChecksum;

  /**
   * Creates a new {@link OffHeapWALIndexHeader} that contains a copy of the data in the provided
   * {@link MemorySegment}. This segment is a read-only view of the WAL index off-heap memory.
   *
   * @param segment The segment of memory the data is stored in.
   */
  private OffHeapWALIndexHeader(final MemorySegment segment) {
    this.lastCommitFrame =
        requireNonNull(segment, "segment must be non-null.")
            .get(JAVA_INT, LAST_COMMIT_FRAME_OFFSET);
    this.cursor = new WALCursor(segment.get(JAVA_INT, WAL_CURSOR_OFFSET));
    this.randomSalt = segment.get(JAVA_INT, RANDOM_SALT_OFFSET);
    this.sequentialSalt = segment.get(JAVA_INT, SEQ_SALT_OFFSET);
    this.dbSize = segment.get(JAVA_LONG, DB_SIZE_OFFSET);
    this.cumulativeChecksum = segment.get(JAVA_LONG, CHECKSUM_OFFSET);
  }

  /**
   * Creates a new builder for {@link OffHeapWALIndexHeader} instances. The given {@code memory}
   * must match the {@link MemoryLayout} of an {@link OffHeapWALIndexHeader}.
   *
   * @param memory The backing memory segment.
   * @return A new {@link Builder} instance.
   * @throws NullPointerException If {@code memory} is {@code null}.
   * @throws IllegalArgumentException If {@code memory} is incompatible with {@link
   *     OffHeapWALIndexHeader#LAYOUT}.
   */
  public static Builder newBuilder(final MemorySegment memory) {
    return new Builder(memory);
  }

  /** {@inheritDoc} */
  @Override
  public int lastCommitFrame() {
    return this.lastCommitFrame;
  }

  /** {@inheritDoc} */
  @Override
  public int cursor() {
    return this.cursor.position();
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
  public long dbSize() {
    return this.dbSize;
  }

  /** {@inheritDoc} */
  @Override
  public long cumulativeChecksum() {
    return this.cumulativeChecksum;
  }

  /** {@inheritDoc} */
  @Override
  public void notifyAppended(final WALFrame frame, final int frameIndex) {
    ensureAtLeastZero(frameIndex);
    requireNonNull(frame, "frame must be non-null.");

    this.cursor.increment();
    this.cumulativeChecksum = frame.checksum();

    // We just increased the database size. If this is a commit, this size will be written to the
    // WALHeader. If this is not a commit, the incremented size will only be visible to the current
    // thread. If the commit fails, this on-heap header copy is discarded.
    this.dbSize++;
    if (frame.isCommit()) {
      this.lastCommitFrame = frameIndex;
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final OffHeapWALIndexHeader that = (OffHeapWALIndexHeader) o;
    return lastCommitFrame == that.lastCommitFrame
        && randomSalt == that.randomSalt
        && sequentialSalt == that.sequentialSalt
        && cumulativeChecksum == that.cumulativeChecksum
        && Objects.equals(cursor, that.cursor);
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return Objects.hash(cursor, lastCommitFrame, randomSalt, sequentialSalt, cumulativeChecksum);
  }

  /**
   * A builder for {@link OffHeapWALIndexHeader} instances.
   *
   * @author houthacker
   */
  public static final class Builder {

    /** The backing memory segment. */
    private final MemorySegment memory;

    private Builder(final MemorySegment memory) {
      if (!isCompatibleWith(memory, OffHeapWALIndexHeader.LAYOUT)) {
        throw new IllegalArgumentException(
            "MemorySegment is not compatible with OffHeapWALIndexHeader.LAYOUT");
      }

      this.memory = memory;
    }

    /**
     * Writes the given index of the last valid wal-frame to the backing memory segment.
     *
     * @param lastCommitFrame The index of the last valid commit frame.
     * @return This {@link Builder} instance.
     */
    public Builder lastCommitFrame(final int lastCommitFrame) {
      this.memory.set(JAVA_INT, LAST_COMMIT_FRAME_OFFSET, lastCommitFrame);

      return this;
    }

    /**
     * Writes the given cursor value to the backing memory segment.
     *
     * @param cursor The position of the next WAL frame to read.
     * @return This {@link Builder} instance.
     */
    public Builder cursor(final int cursor) {
      this.memory.set(JAVA_INT, WAL_CURSOR_OFFSET, cursor);

      return this;
    }

    /**
     * Writes the given random salt value to the backing memory segment. This salt is reset to a new
     * random value on each checkpoint.
     *
     * @param randomSalt The random salt.
     * @return This {@link Builder} instance.
     */
    public Builder randomSalt(final int randomSalt) {
      this.memory.set(JAVA_INT, RANDOM_SALT_OFFSET, randomSalt);

      return this;
    }

    /**
     * Writes the given sequential salt to the backing memory segment. This salt is incremented by
     * {@code 1} on each checkpoint.
     *
     * @param sequentialSalt The sequential salt.
     * @return This {@link Builder} instance.
     */
    public Builder sequentialSalt(final int sequentialSalt) {
      this.memory.set(JAVA_INT, SEQ_SALT_OFFSET, sequentialSalt);

      return this;
    }

    /**
     * Writes the given cumulative checksum to the backing memory segment. This checksum is
     * recalculated whenever a new frame is appended to the wal.
     *
     * @param cumulativeChecksum The cumulative checksum.
     * @return This {@link Builder} instance.
     */
    public Builder cumulativeChecksum(final long cumulativeChecksum) {
      this.memory.set(JAVA_LONG, CHECKSUM_OFFSET, cumulativeChecksum);

      return this;
    }

    /**
     * Writes the given database size in pages to the backing memory segment. This size is then
     * updated on each commit.
     *
     * @param dbSize The database size in pages.
     * @return This {@link Builder} instance.
     */
    public Builder dbSize(final long dbSize) {
      this.memory.set(JAVA_LONG, DB_SIZE_OFFSET, dbSize);

      return this;
    }

    /**
     * Creates a new {@link OffHeapWALIndexHeader} based on the provided backing memory segment.
     *
     * @return The new {@link OffHeapWALIndexHeader}.
     */
    public OffHeapWALIndexHeader build() {
      return new OffHeapWALIndexHeader(this.memory);
    }
  }
}
