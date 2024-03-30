package dev.luxor.server.wal.local;

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
import dev.luxor.server.wal.WALLockType;
import dev.luxor.server.wal.WALWriteException;
import dev.luxor.server.wal.WriteAheadLog;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A local write-ahead-log implementation with an off-heap index. This is the default WAL used by
 * {@code luxor}, and it supports the durability requirements of ACID.
 *
 * @author houthacker
 */
@SuppressWarnings("PMD.TooManyMethods") // Refactor?
public final class LocalWAL implements WriteAheadLog {

  private static final Logger log = LoggerFactory.getLogger(LocalWAL.class);

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

  @SuppressWarnings({
    "PMD.AvoidLiteralsInIfCondition", // Allowed for empty check.
    "java:S2095", // The LuxorFile is closed when the WAL is closed.
    "java:S2093" // The WAL must not be closed here.
  })
  private static LocalWAL openNew(
      final Path databasePath, final Path walPath, final Path walIndexPath) throws IOException {
    log.debug("Opening new WAL at {}.", walPath);
    INIT_LOCK.lock();
    try {

      if (Files.size(databasePath) == 0L) {
        // Open the WAL- and index file, throwing an exception if someone beat us to it and did
        // create the WAL file in the meantime. In that case, clients should just retry to open the
        // WAL.
        final LuxorFile wal =
            LuxorFile.open(
                walPath,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE_NEW);
        return new LocalWAL(
            wal,
            OffHeapWALIndex.buildInitial(
                0L,
                LuxorFile.open(
                    walIndexPath,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE_NEW)));
      } else {
        throw new UnsupportedOperationException("Not implemented");
      }

    } finally {
      INIT_LOCK.unlock();
    }
  }

  /** {@inheritDoc} */
  @Override
  public LocalWALHeader header() throws IOException, CorruptWALException {
    return LocalWALHeader.readFromFile(this.file);
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
  public int frameIndexOf(final long pageNumber) {
    return this.index.findFrameIndexOf(pageNumber);
  }

  /** {@inheritDoc} */
  @Override
  public ByteBuffer pageAt(final int frameIndex) throws IOException, NoSuchPageException {

    // Check if the given frame is within the file bounds from the point of view of the calling
    // thread.
    if (frameIndex <= this.index.header().lastCommitFrame()) {
      // The offset also skips over the frame header, pointing directly to the page content.
      final long offset =
          LocalWALHeader.BYTES + ((long) frameIndex * WALFrame.BYTES) + WALFrame.HEADER_BYTES;

      final ByteBuffer buf = ByteBuffer.allocate(Page.BYTES);
      final int bytesRead = this.file.read(buf, offset);
      if (bytesRead == Page.BYTES) {
        return buf;
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
              .checksum(
                  new FNV1a(header.cumulativeChecksum())
                      .iterate(page.index())
                      .iterate(commit)
                      .iterate(header.randomSalt())
                      .iterate(header.sequentialSalt())
                      .iterate(pageData, 0, Page.BYTES)
                      .state())
              .build();

      // Calculate the offset at which the frame must be written.
      final int cursor = header.cursor();
      final long offset = LocalWALHeader.BYTES + (long) cursor * WALFrame.BYTES;

      // Then write the frame.
      if (log.isTraceEnabled()) {
        log.trace(
            "Writing frame(index={}, page={} commit={}) to WAL", cursor, page.index(), commit);
      }
      this.file.write(frame.header(), offset);
      this.file.write(pageData.rewind(), offset + WALFrame.HEADER_BYTES);

      // Update the index accordingly so the written page can be read by other processes.
      this.index.notifyAppended(frame, cursor);

      // Ensure durability by flushing the file contents to its storage device after every commit.
      if (frame.isCommit()) {
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
