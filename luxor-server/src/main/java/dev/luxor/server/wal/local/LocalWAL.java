package dev.luxor.server.wal.local;

import static dev.luxor.server.shared.Ensure.ensureAtLeastZero;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;

import dev.luxor.server.algo.FNV1a;
import dev.luxor.server.concurrent.LockFailedException;
import dev.luxor.server.io.CorruptPageException;
import dev.luxor.server.io.LuxorFile;
import dev.luxor.server.io.NoSuchPageException;
import dev.luxor.server.io.Page;
import dev.luxor.server.wal.CorruptWALException;
import dev.luxor.server.wal.StaleWALException;
import dev.luxor.server.wal.TransientWALWriteException;
import dev.luxor.server.wal.WALFrame;
import dev.luxor.server.wal.WALHeader;
import dev.luxor.server.wal.WALLockType;
import dev.luxor.server.wal.WALSpliterator;
import dev.luxor.server.wal.WALWriteException;
import dev.luxor.server.wal.WriteAheadLog;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileLock;
import java.nio.channels.NonWritableChannelException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A local write-ahead-log implementation with an off-heap index. This is the default WAL used by
 * {@code luxor}, and it supports the durability requirements of ACID.
 *
 * @author houthacker
 */
@SuppressWarnings({"PMD.TooManyMethods", "PMD.ExcessiveImports"})
public final class LocalWAL implements WriteAheadLog {

  private static final Logger log = LoggerFactory.getLogger(LocalWAL.class);

  /** The offset (in bytes) of the {@link LocalWALHeader} within the WAL file. */
  public static final long HEADER_OFFSET = 0L;

  /** The global lock that must be used when creating a new WAL file. */
  private static final ReentrantLock INIT_LOCK = new ReentrantLock();

  /** The actual file containing the WAL. */
  private final LuxorFile file;

  /** A copy of the main WAL index which is stored in shared memory. */
  private final OffHeapWALIndex index;

  /**
   * Creates a new {@link LocalWAL} instance. The file at {@code databasePath} must exist before the
   * WAL-file is created.
   *
   * @param wal The wal file.
   * @param index The wal-index file.
   * @throws java.util.ConcurrentModificationException If the WAL-index is modified while it is
   *     read.
   */
  private LocalWAL(final LuxorFile wal, final OffHeapWALIndex index) {
    this.file = wal;
    this.index = index;
  }

  /**
   * Opens the WAL for the database at {@code databasePath}. If required, the WAL file and -index
   * are initialized. If the WAL file appears to be initialized already, a recovery check is
   * performed. If no recovery is necessary, this method just returns the new WAL instance.
   * Otherwise, since recovery is not yet implemented, this method throws an exception.
   *
   * @param databasePath The path of the related database.
   * @return A new {@link LocalWAL} instance.
   * @throws IllegalStateException If a new WAL is opened while some other thread already owns the
   *     initializer lock. In this case, callers should just retry to open the WAL.
   * @throws java.util.ConcurrentModificationException If the WAL index is modified while reading it
   *     from shared memory, or if changes to the WAL contents are detected after obtaining the
   *     exclusive lock.
   * @throws java.nio.file.FileAlreadyExistsException If the WAL-file is created by another thread
   *     or process after this method determined it doesn't exist and must be created.
   * @throws IOException If the database file does not exist, the WAL-file cannot be opened or if *
   *     the WAL index cannot be opened or mapped into shared memory.
   * @throws CorruptWALException If the WAL header is corrupt or if it cannot be read fully.
   */
  public static LocalWAL open(final Path databasePath) throws IOException, CorruptWALException {
    final String dbPathString =
        requireNonNull(databasePath, "databasePath must be non-null").toRealPath().toString();

    final Path walPath = Path.of(String.format("%s-wal", dbPathString));
    final Path walIndexPath = Path.of(String.format("%s-shm", dbPathString));

    final LocalWAL wal;
    if (Files.exists(walPath)) {
      // If the WAL exists, there are a few scenarios:
      //
      // 1. The database was previously opened normally (most common case)
      // 2. Another thread is currently creating a new WAL
      // 3. The WAL is a stale leftover from a previous system failure.
      //
      // It is hard to distinguish between these cases, because either one can be an intermediate
      // state of the other.
      // Because of that, the wal index is opened normally, and only throws an exception if it
      // detects that a recovery is required.
      wal = openExisting(walPath, walIndexPath);
    } else {
      wal = openNew(databasePath, walPath, walIndexPath);
    }

    return wal;
  }

  /**
   * Creates a new {@link LocalWAL} instance based on an existing WAL.
   *
   * @param walPath The path to the WAL file.
   * @param walIndexPath The path to the WAL index file.
   * @return The resulting {@link LocalWAL} instance.
   * @throws IOException If an I/O error occurs while reading or writing any of the provided paths.
   * @throws CorruptWALException If the WAL checksum is invalid.
   */
  @SuppressWarnings("java:S2095") // The LuxorFile is closed when the WAL is closed.
  private static LocalWAL openExisting(final Path walPath, final Path walIndexPath)
      throws IOException, CorruptWALException {
    log.debug("Opening existing WAL at {}.", walPath);

    final LuxorFile wal =
        LuxorFile.open(
            walPath, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
    return new LocalWAL(
        wal,
        OffHeapWALIndex.buildFromExisting(
            wal,
            LuxorFile.open(
                walIndexPath,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE)));
  }

  /**
   * Creates a new WAL at the given {@code walPath} and writes an initial header to it. Creating a
   * new WAL when a database file contains any data is not yet supported.
   *
   * @param databasePath The path to the database file.
   * @param walPath The path to the WAL file.
   * @param walIndexPath The path to the WAL index file.
   * @return The resulting {@link LocalWAL} instance.
   * @throws IOException If an I/O error occurs while reading or writing any of the provided paths.
   * @throws CorruptWALException If the initial WAL header cannot be fully written to the WAL file.
   */
  @SuppressWarnings({
    "PMD.AvoidLiteralsInIfCondition", // Allowed for empty check.
    "java:S2095", // The LuxorFile is closed when the WAL is closed.
  })
  private static LocalWAL openNew(
      final Path databasePath, final Path walPath, final Path walIndexPath)
      throws IOException, CorruptWALException {
    log.debug("Opening new WAL at {}.", walPath);
    if (INIT_LOCK.tryLock()) {
      try {

        if (Files.size(databasePath) == 0L) {
          // Open the WAL- and index file, throwing an exception if someone beat us to it and did
          // create the WAL file in the meantime.
          final LuxorFile walFile =
              LuxorFile.open(
                  walPath,
                  StandardOpenOption.READ,
                  StandardOpenOption.WRITE,
                  StandardOpenOption.CREATE_NEW);
          final LuxorFile walIndexFile =
              LuxorFile.open(
                  walIndexPath,
                  StandardOpenOption.READ,
                  StandardOpenOption.WRITE,
                  StandardOpenOption.CREATE_NEW);

          // Then write the WAL header and -index.
          return writeWALHeaderAndIndex(walFile, walIndexFile);
        }

        // Creating a new WAL based on a non-empty database is not yet supported.
        throw new UnsupportedOperationException("Not implemented");
      } finally {
        INIT_LOCK.unlock();
      }
    }

    throw new IllegalStateException("Cannot obtain initializer lock.");
  }

  /**
   * Writes a new {@link LocalWALHeader} to the WAL and creates a new {@link OffHeapWALIndex} based
   * on that new WAL header. If either of the files is not empty, an exception is thrown to prevent
   * WAL corruption.
   *
   * @param walFile The WAL file.
   * @param walIndexFile The WAL index file.
   * @return A new {@link LocalWAL} instance.
   * @throws IOException If an I/O error occurs when reading from, or writing to the WAL (index).
   * @throws CorruptWALException If the WAL checksum is invalid, or if the WAL header cannot be
   *     completely written to the WAL file.
   */
  @SuppressWarnings({
    "java:S2245", // The random number generator is not used in a security context.
    "java:S2093" // Cannot close the WAL file we're creating here.
  })
  private static LocalWAL writeWALHeaderAndIndex(
      final LuxorFile walFile, final LuxorFile walIndexFile)
      throws IOException, CorruptWALException {

    // First, get both a read- and a write lock on walIndexFile.
    try (FileLock readLock = walIndexFile.tryLock(WALLockType.SHARED.offset(), 1L, true);
        FileLock writeLock = walIndexFile.tryLock(WALLockType.EXCLUSIVE.offset(), 1L, false)) {
      if (nonNull(readLock) && nonNull(writeLock)) {

        // At this point we know we were the ones that created both files.
        // Now re-ensure that they're empty.
        if (walFile.size() == 0L && walIndexFile.size() == 0L) {

          // Create an initial WAL header and write it to the WAL file.
          final ThreadLocalRandom rng = ThreadLocalRandom.current();
          final LocalWALHeader walHeader =
              LocalWALHeader.newBuilder()
                  .magic(WALHeader.MAGIC)
                  .dbSize(0L)
                  .checkpointSequence(0)
                  .randomSalt(rng.nextInt())
                  .sequentialSalt(rng.nextInt())
                  .calculateChecksum()
                  .build();
          if (walFile.write(walHeader.asByteBuffer(), 0L) != LocalWALHeader.BYTES) {
            throw new CorruptWALException(
                "Cannot create new WAL: could not write initial WAL header.");
          }
          walFile.sync();

          // And then create the WAL index based on the values from the WAL header.
          final OffHeapWALIndex index =
              OffHeapWALIndex.buildInitial(
                  walHeader.dbSize(),
                  walHeader.randomSalt(),
                  walHeader.sequentialSalt(),
                  walIndexFile);
          return new LocalWAL(walFile, index);
        }

        throw new IllegalStateException("Cannot create new WAL: it is not empty.");
      }

      throw new IllegalStateException("Cannot create new WAL: error obtaining initializer locks.");
    }
  }

  /** {@inheritDoc} */
  @Override
  public LocalWALHeader header() throws IOException, CorruptWALException {
    return LocalWALHeader.readFromFile(this.file, HEADER_OFFSET);
  }

  /** {@inheritDoc} */
  @Override
  public WALSpliterator spliterator() throws CorruptWALException, IOException {
    return new LocalWALSpliterator(this.index.header(), this.file);
  }

  /** {@inheritDoc} */
  @Override
  public void beginReadTransaction() throws LockFailedException {
    if (this.index.isStale()) {
      this.index.reload();
    }

    this.index.lock(WALLockType.SHARED);
  }

  /** {@inheritDoc} */
  @Override
  public void endReadTransaction() {
    this.index.unlock();
  }

  /** {@inheritDoc} */
  @Override
  public int frameIndexOf(final long pageIndex) {
    return this.index.findFrameIndexOf(pageIndex);
  }

  /** {@inheritDoc} */
  @Override
  public ByteBuffer pageAt(final int frameIndex) throws IOException, NoSuchPageException {
    ensureAtLeastZero(frameIndex);
    final WALLockType lockType = this.index.currentLock();
    if (lockType.mask() < WALLockType.SHARED.mask() && log.isWarnEnabled()) {
      log.warn("Reading WAL frame with unexpected lock type {}.", lockType.name());
    }

    // Check if the given frame is within the file bounds from the point of view of the calling
    // thread.
    if (frameIndex <= this.index.header().lastCommitFrame()) {
      // The offset also skips over the frame header, pointing directly to the page content.
      final long offset =
          LocalWALHeader.BYTES + ((long) frameIndex * WALFrame.BYTES) + WALFrame.HEADER_BYTES;

      final ByteBuffer buf = ByteBuffer.allocate(Page.BYTES);
      final int bytesRead = this.file.read(buf, offset);
      if (bytesRead == Page.BYTES) {
        return buf.rewind();
      }

      throw new CorruptPageException(
          String.format(
              "Could only read %d/%d bytes of page at WAL frame %d.",
              bytesRead, Page.BYTES, frameIndex));
    }

    throw new NoSuchPageException(String.format("Requested frame %d does not exist.", frameIndex));
  }

  /** {@inheritDoc} */
  @Override
  public void beginWriteTransaction() throws LockFailedException, StaleWALException {
    this.index.lock(WALLockType.EXCLUSIVE);

    if (this.index.isStale()) {
      // Another thread wrote to the WAL in the time between the shared lock and the exclusive lock
      // were obtained. Unlock the read- and write locks and have the client retry.
      this.index.unlock();
      throw new StaleWALException(
          "Cannot begin write transaction: WAL contents changed since read transaction started.");
    }
  }

  /** {@inheritDoc} */
  @Override
  public void endWriteTransaction() {
    this.index.unlock();
  }

  /** {@inheritDoc} */
  @Override
  @SuppressWarnings("PMD.CyclomaticComplexity")
  public void writePage(final Page page, final boolean commit) throws TransientWALWriteException {
    final WALLockType lockType = this.index.currentLock();
    if (lockType != WALLockType.EXCLUSIVE && log.isWarnEnabled()) {
      log.warn("Writing WAL frame with unexpected lock type {}.", lockType.name());
    }

    try {
      requireNonNull(page, "page must be non-null");

      final OffHeapWALIndexHeader header = this.index.header();
      final ByteBuffer pageData = page.readOnlyData();
      final WALFrame frame =
          WALFrame.newBuilder()
              .pageIndex(page.index())
              .commit(commit)
              .randomSalt(header.randomSalt())
              .sequentialSalt(header.sequentialSalt())
              .page(pageData)
              .calculateChecksum(new FNV1a(header.cumulativeChecksum()))
              .build();

      // Calculate the offset at which the frame must be written.
      final int cursor = header.cursor();
      final long offset = LocalWALHeader.BYTES + (long) cursor * WALFrame.BYTES;

      // Then write the frame.
      if (log.isTraceEnabled()) {
        log.trace("Writing WAL frame(index={}, page={} commit={})", cursor, page.index(), commit);
      }
      this.file.write(frame.header(), offset);
      this.file.write(pageData.rewind(), offset + WALFrame.HEADER_BYTES);

      // Update the index accordingly so the written page can be read by other processes.
      this.index.notifyAppended(frame, cursor);

      // Ensure durability by flushing the file contents to its storage device after every commit.
      if (frame.isCommit()) {
        final LocalWALHeader walHeader = this.header();

        this.file.write(
            LocalWALHeader.newBuilder(walHeader)
                .dbSize(this.index.header().dbSize())
                .calculateChecksum()
                .build()
                .asByteBuffer(),
            HEADER_OFFSET);

        this.index.sync();
        this.file.sync();
      }

    } catch (NonWritableChannelException e) {
      throw new WALWriteException("WAL not opened for writing.", e);
    } catch (ClosedByInterruptException e) {
      // A ClosedByInterruptException indicates that another thread interrupted our write for some
      // important reason or other, for example when forcibly shutting down Luxor.
      throw new WALWriteException("WAL closed because of an interrupt.", e);
    } catch (AsynchronousCloseException e) {
      // When an AsynchronousCloseException is thrown, this also indicates that another thread
      // shares the same file instance and closed it. This is not recommended, but not enforced.
      throw new TransientWALWriteException(
          "WAL closed by another thread. Consider providing each thread with its own WriteAheadLog instance",
          e);
    } catch (ClosedChannelException e) {
      // The underlying FileChannel has been closed due to a reason other than the two cases above.
      throw new WALWriteException("WAL closed.", e);
    } catch (IOException e) {
      throw new TransientWALWriteException("Could not write to the WAL.", e);
    } catch (CorruptWALException e) {
      throw new WALWriteException("WAL is corrupted, therefore not appending page.", e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    try (LuxorFile ignored = this.file) { // use try-with-resources to close the file.
      this.index.close();
    }
  }
}
