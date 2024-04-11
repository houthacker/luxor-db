package dev.luxor.server.wal;

import static dev.luxor.server.shared.Ensure.ensureAtLeastOne;
import static java.util.Objects.requireNonNull;

import dev.luxor.server.algo.FNV1a;
import dev.luxor.server.pager.Page;
import java.nio.ByteBuffer;

/**
 * A record of a single revised database page.
 *
 * @author houthacker
 */
@SuppressWarnings(
    "PMD.DataClass") // This could even be a record, but then we'd miss out of all the great
// documentation.
public final class WALFrame {

  /** The byte size of a WAL frame header when serialized. */
  public static final int HEADER_BYTES = 32;

  /** The byte size of a WAL frame when serialized. */
  public static final int BYTES = HEADER_BYTES + Page.BYTES;

  /** The page number of the page in the database. */
  private final long pageIndex;

  /** Whether this is a commit frame. */
  private final boolean commit;

  /** The random frame salt. */
  private final int randomSalt;

  /** The sequential frame salt. */
  private final int sequentialSalt;

  /** The cumulative frame checksum. */
  private final long checksum;

  /** The page data. */
  private final ByteBuffer page;

  /**
   * Creates a new {@link WALFrame} with the given parameters.
   *
   * @param pageIndex The index of the page contained in the frame.
   * @param commit {@code true} if this is a commit frame, {@code false} otherwise.
   * @param randomSalt The random salt of the WAL at the time the frame was created.
   * @param sequentialSalt The sequential salt of the WAL at the time the frame was created.
   * @param checksum The cumulative checksum of all preceding frames including the current one being
   *     created.
   * @param page The page data.
   */
  private WALFrame(
      final long pageIndex,
      final boolean commit,
      final int randomSalt,
      final int sequentialSalt,
      final long checksum,
      final ByteBuffer page) {
    if (requireNonNull(page, "page must be non-null").limit() != Page.BYTES) {
      throw new IllegalArgumentException("page must contain exactly Page.BYTES.");
    }

    this.pageIndex = ensureAtLeastOne(pageIndex);
    this.commit = commit;
    this.randomSalt = randomSalt;
    this.sequentialSalt = sequentialSalt;
    this.checksum = checksum;
    this.page = page;
  }

  /**
   * Creates a new {@link Builder}.
   *
   * @return The new {@link Builder}.
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * The index of the contained page.
   *
   * @return The page index.
   */
  public long pageIndex() {
    return this.pageIndex;
  }

  /**
   * Whether this frame is a commit frame.
   *
   * @return Whether this frame is a commit frame.
   */
  public boolean isCommit() {
    return this.commit;
  }

  /**
   * A copy of the random salt from the WAL header.
   *
   * @return The random salt.
   */
  public int randomSalt() {
    return this.randomSalt;
  }

  /**
   * A copy of the sequential salt from the WAL header.
   *
   * @return The sequential salt.
   */
  public int sequentialSalt() {
    return this.sequentialSalt;
  }

  /**
   * The cumulative checksum of all frames up to and including this frame.
   *
   * @return The cumulative checksum.
   */
  public long checksum() {
    return this.checksum;
  }

  /**
   * A read-only {@link ByteBuffer} containing the page data.
   *
   * @return The page data.
   */
  public ByteBuffer page() {
    return this.page.rewind().asReadOnlyBuffer();
  }

  /**
   * Creates a new {@link ByteBuffer} containing the frame header.
   *
   * @return The frame header as a byte buffer.
   */
  public ByteBuffer header() {
    return ByteBuffer.allocate(HEADER_BYTES)
        .putLong(this.pageIndex)
        .put(this.commit ? (byte) 1 : 0)
        .putInt(this.randomSalt)
        .putInt(this.sequentialSalt)
        .putLong(this.checksum)
        .rewind();
  }

  /** Builder for {@link WALFrame} instances. */
  public static final class Builder {

    /**
     * mask for builder fields, in order. The field 'commit' is considered to be set, because its
     * default value for a frame is the same as a boolean default value (false).
     */
    private byte mask = 0x02;

    /** The frame page index. */
    private long pageIndex;

    /** Whether this is a commit frame; defaults to {@code false}. */
    private boolean commit;

    /** The random salt. */
    private int randomSalt;

    /** The sequential salt. */
    private int sequentialSalt;

    /** The cumulative checksum. */
    private long checksum;

    /** The page data. */
    private ByteBuffer page;

    /** Creates a new {@link Builder} instance. */
    private Builder() {
      /* Only instantiate from containing class */
    }

    /**
     * Sets the number of the page contained in the {@link WALFrame}. Must be {@code >= 1}. This
     * field is required.
     *
     * @param pageIndex The page number to set.
     * @return This {@link Builder} instance.
     */
    public Builder pageIndex(final long pageIndex) {
      this.pageIndex = ensureAtLeastOne(pageIndex);
      this.mask |= 0x01;

      return this;
    }

    /**
     * Sets whether the frame is a commit frame. This field is optional.
     *
     * @param commit {@code true} if the frame is a commit frame, {@code false} otherwise.
     * @return This {@link Builder} instance.
     */
    public Builder commit(final boolean commit) {
      this.commit = commit;
      this.mask |= 0x02;

      return this;
    }

    /**
     * Sets the random salt for the frame. This value must be copied from the {@link
     * WALIndexHeader}. This field is required.
     *
     * @param randomSalt The random salt assigned to the frame.
     * @return This {@link Builder} instance.
     */
    public Builder randomSalt(final int randomSalt) {
      this.randomSalt = randomSalt;
      this.mask |= 0x04;

      return this;
    }

    /**
     * Sets the sequential salt for the frame. This value must be copied from the {@link
     * WALIndexHeader}. This field is required.
     *
     * @param sequentialSalt The sequential salt assigned to the frame.
     * @return This {@link Builder} instance.
     */
    public Builder sequentialSalt(final int sequentialSalt) {
      this.sequentialSalt = sequentialSalt;
      this.mask |= 0x08;

      return this;
    }

    /**
     * Sets the checksum for the frame. This value must be the cumulative checksum of all preceding
     * frames, including this frame. This field is required.
     *
     * @param checksum The checksum assigned to the frame.
     * @return This {@link Builder} instance.
     */
    public Builder checksum(final long checksum) {
      this.checksum = checksum;
      this.mask |= 0x10;

      return this;
    }

    /**
     * Calculates the checksum for this frame using {@code cumulativeChecksum} as the initial
     * checksum state.
     *
     * @param cumulativeChecksum The cumulative checksum of the preceding frame.
     * @return This {@link Builder} instance.
     */
    public Builder calculateChecksum(final FNV1a cumulativeChecksum) {
      return this.checksum(
          cumulativeChecksum
              .iterate(this.pageIndex)
              .iterate(this.commit)
              .iterate(this.randomSalt)
              .iterate(this.sequentialSalt)
              .iterate(this.page, 0, this.page.limit())
              .state());
    }

    /**
     * Reads the {@link WALFrame} header fields from {@code header}.
     *
     * @param header The {@link ByteBuffer} containing the header field values.
     * @return This {@link Builder} instance.
     */
    public Builder header(final ByteBuffer header) {
      if (requireNonNull(header, "header must be non-null.").limit() == HEADER_BYTES) {
        this.pageIndex(header.getLong());
        this.commit(header.get() == (byte) 1);
        this.randomSalt(header.getInt());
        this.sequentialSalt(header.getInt());
        this.checksum(header.getLong());

        return this;
      }

      throw new IllegalArgumentException(
          "header buffer must contain exactly WALFrame.HEADER_BYTES bytes.");
    }

    /**
     * Sets the {@link ByteBuffer} containing the page data for the frame. This buffer must have a
     * limit of exactly {@link Page#BYTES}. This field is required.
     *
     * @param page The page data.
     * @return This {@link Builder} instance.
     * @throws NullPointerException If {@code page} is null.
     * @throws IllegalArgumentException If {@code page.limit() != Page.BYTES}.
     */
    public Builder page(final ByteBuffer page) {
      if (requireNonNull(page, "page must be non-null.").limit() == Page.BYTES) {
        this.page = page.rewind();
        this.mask |= 0x20;

        return this;
      }

      throw new IllegalArgumentException("page buffer must contain exactly Page.BYTES.");
    }

    /**
     * Builds the {@link WALFrame} and returns it.
     *
     * @return The new {@link WALFrame}.
     * @throws IllegalStateException If not all required fields have been set.
     */
    public WALFrame build() {
      if ((this.mask ^ 0x3f) == 0) {
        return new WALFrame(
            this.pageIndex,
            this.commit,
            this.randomSalt,
            this.sequentialSalt,
            this.checksum,
            this.page);
      }

      throw new IllegalStateException(
          "Cannot create WALFrame: not all required fields have been set.");
    }
  }
}
