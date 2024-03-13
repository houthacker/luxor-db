package dev.luxor.server.wal;

import static java.util.Objects.requireNonNull;

import dev.luxor.server.concurrent.LockFailedException;
import dev.luxor.server.io.LuxorFile;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Shared-memory-backed write-ahead-log implementation. This is the default WAL used by {@code
 * luxor}, and it supports the durability requirements of ACID.
 *
 * @author houthacker
 */
public class ProcessSharedWAL implements WriteAheadLog {

  /** The path to the related database file. */
  private final Path path;

  /** The actual file containing the WAL. */
  private final LuxorFile file;

  /** A copy of the main WAL index which is stored in shared memory. */
  private final OffHeapWALIndex index;

  /**
   * Creates a new {@link ProcessSharedWAL} instance.
   *
   * @param databasePath The full path to the related database file.
   * @throws IOException If the database file does not exist, the WAL-file cannot be opened or if
   *     the WAL index cannot be opened or mapped into shared memory.
   * @throws java.util.ConcurrentModificationException If the WAL-index is modified while it is
   *     read.
   */
  public ProcessSharedWAL(final Path databasePath) throws IOException {
    final String dbPathString =
        requireNonNull(databasePath, "databasePath must be non-null").toRealPath().toString();

    this.path = Path.of(String.format("%s-wal", dbPathString));
    this.file =
        LuxorFile.open(
            this.path,
            StandardOpenOption.READ,
            StandardOpenOption.WRITE,
            StandardOpenOption.CREATE);

    final Path shmPath = Path.of(String.format("%s-shm", dbPathString));
    this.index =
        new OffHeapWALIndex(
            LuxorFile.open(
                shmPath,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE));
  }

  /** {@inheritDoc} */
  @Override
  public void beginReadTransaction() throws LockFailedException {
    if (!this.index.isCurrent()) {
      try {
        this.index.reload();
      } catch (IOException e) {
        throw new LockFailedException("Could not acquire lock, reloading table data failed.", e);
      }
    }

    this.index.lock(WALLockType.shared);
  }

  /** {@inheritDoc} */
  @Override
  public void endReadTransaction() {
    this.index.unlock();
  }
}
