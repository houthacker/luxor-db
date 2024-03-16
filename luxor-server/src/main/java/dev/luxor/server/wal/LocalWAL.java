package dev.luxor.server.wal;

import static java.util.Objects.requireNonNull;

import dev.luxor.server.concurrent.LockFailedException;
import dev.luxor.server.io.CorruptPageException;
import dev.luxor.server.io.LuxorFile;
import dev.luxor.server.io.NoSuchPageException;
import dev.luxor.server.io.Page;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A local write-ahead-log implementation with an off-heap index. This is the default WAL used by
 * {@code luxor}, and it supports the durability requirements of ACID.
 *
 * @author houthacker
 */
public class LocalWAL implements WriteAheadLog {

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
    this.file = requireNonNull(wal, "wal must be non-null.");
    this.index = requireNonNull(index, "index must be non-null.");
  }

  /**
   * Initializes this instance by writing an initial {@link WALHeader}.
   *
   * @return this {@link LocalWAL} instance.
   */
  private LocalWAL initialize() throws IOException {
    this.file.write(LocalWALHeader.createDefault().asByteBuffer(), 0L);
    return this;
  }

  /**
   * Opens the WAL for the database at {@code databasePath}. If required, the WAL file and -index
   * are initialized. If the WAL file appears to be initialized already, a recovery check is
   * performed. If no recovery is necessary, this method just returns the new WAL instance.
   * Otherwise, since recovery is not yet implemented, this method throws an exception.
   *
   * @param databasePath The path of the related database.
   * @return A new {@link LocalWAL} instance.
   * @throws java.nio.file.FileAlreadyExistsException If the WAL-file is created by another thread
   *     or process after this method determined it doesn't exist and must be created.
   * @throws IOException If the database file does not exist, the WAL-file cannot be opened or if *
   *     the WAL index cannot be opened or mapped into shared memory.
   * @throws StaleWALException If WAL file exists, but is a stale leftover from a previous system
   *     failure.
   */
  @SuppressWarnings("java:S2095") // The underlying LuxorFile instances are closed with the WAL.
  public static LocalWAL open(final Path databasePath) throws IOException {
    final String dbPathString =
        requireNonNull(databasePath, "databasePath must be non-null").toRealPath().toString();

    final Path walPath = Path.of(String.format("%s-wal", dbPathString));
    final Path walIndexPath = Path.of(String.format("%s-shm", dbPathString));
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
      return new LocalWAL(
          LuxorFile.open(
              walPath,
              StandardOpenOption.READ,
              StandardOpenOption.WRITE,
              StandardOpenOption.CREATE),
          new OffHeapWALIndex(
              LuxorFile.open(
                  walIndexPath,
                  StandardOpenOption.READ,
                  StandardOpenOption.WRITE,
                  StandardOpenOption.CREATE)));
    } else {
      INIT_LOCK.lock();
      try {
        // Open the WAL- and index file, throwing an exception if someone beat us to it and did
        // create the WAL file in the meantime. In that case, clients should just retry to open the
        // WAL.
        return new LocalWAL(
                LuxorFile.open(
                    walPath,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE_NEW),
                new OffHeapWALIndex(
                    LuxorFile.open(
                        walIndexPath,
                        StandardOpenOption.READ,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.CREATE_NEW)))

            // Write a default header to the file after it's opened.
            .initialize();
      } finally {
        INIT_LOCK.unlock();
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public LocalWALHeader header() throws IOException, CorruptWALException {
    return LocalWALHeader.createFromFile(this.file);
  }

  /** {@inheritDoc} */
  @Override
  public void beginReadTransaction() throws LockFailedException {
    if (!this.index.isCurrent()) {
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
    return this.index.findFrame(pageNumber);
  }

  /** {@inheritDoc} */
  @Override
  public ByteBuffer pageAt(int frameIndex) throws IOException, NoSuchPageException {

    // Check if the given frame is within the file bounds from the point of view of the calling
    // thread.
    if (frameIndex <= this.index.header().lastValidFrame()) {
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
  public void close() throws IOException {
    try {
      this.file.close();
    } finally {
      this.index.close();
    }
  }
}
