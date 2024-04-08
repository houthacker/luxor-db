package dev.luxor.server.wal.local;

import static dev.luxor.server.shared.Ensure.ensureAtLeastZero;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;

import dev.luxor.server.algo.FNV1a;
import dev.luxor.server.concurrent.LockFailedException;
import dev.luxor.server.concurrent.OutOfOrderLockException;
import dev.luxor.server.io.LuxorFile;
import dev.luxor.server.wal.CorruptWALException;
import dev.luxor.server.wal.WALFrame;
import dev.luxor.server.wal.WALIndex;
import dev.luxor.server.wal.WALLockType;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileLock;
import java.util.ConcurrentModificationException;
import java.util.stream.StreamSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link OffHeapWALIndex} stores its data off-heap in a memory-mapped file.
 *
 * @implNote The constructor of this class throws an exception, which makes it vulnerable to
 *     finalizer attacks. To mitigate that, this class is made final. If the need to extend this
 *     class arises, it is obviously required to find another mitigation.
 * @author houthacker
 */
@SuppressWarnings("PMD.TooManyMethods")
public final class OffHeapWALIndex implements WALIndex {

  private static final Logger log = LoggerFactory.getLogger(OffHeapWALIndex.class);

  /**
   * The amount of index headers in an {@link OffHeapWALIndex}. We need two copies to detect
   * concurrent modification.
   */
  private static final int INDEX_HEADERS_SIZE = 2;

  /** The layout of two consecutive wal_index_header structs. */
  private static final MemoryLayout HEADERS_LAYOUT =
      MemoryLayout.sequenceLayout(INDEX_HEADERS_SIZE, OffHeapWALIndexHeader.LAYOUT);

  static {
    // If the byte alignment changes, the main index layout might require padding.
    assert HEADERS_LAYOUT.byteAlignment() == 8;
  }

  /** The name of the 'headers' fiend in the layout of the index struct. */
  private static final String HEADERS_NAME = "headers";

  /** The amount of lock types. Each lock type has its own lock byte in the backing file. */
  private static final int FILE_LOCK_COUNT = 8;

  /** The layout of a wal_index struct in off-heap memory. */
  private static final MemoryLayout LAYOUT =
      MemoryLayout.structLayout(
              HEADERS_LAYOUT.withName(HEADERS_NAME),
              MemoryLayout.sequenceLayout(FILE_LOCK_COUNT, ValueLayout.JAVA_BYTE).withName("locks"))
          .withName("wal_index");

  /** The struct field pointing to the two index header copies. */
  private static final MemoryLayout.PathElement HEADERS_ELEMENT =
      MemoryLayout.PathElement.groupElement(HEADERS_NAME);

  static {

    // Since the alignment requirement of the index layout is so small, we don't really have to
    // take care of aligning our writes to the related MemorySegment. If this alignment changes in
    // the future, we might have to rethink this. The OffHeapHashSet already does this, so that can
    // be used as an example.
    assert LAYOUT.byteAlignment() == 8;
  }

  /** The backing file. */
  private final LuxorFile file;

  /** The memory segment containing the WAL index header and lock bytes. */
  private final MemorySegment memory;

  /** An on-heap copy of the wal index headers. */
  private final OffHeapWALIndexHeader[] headers;

  /** The table containing the frame/page mappings. */
  private final OffHeapHashSet table;

  /** A bit-masked integer storing the lock types held at a given point in time. */
  private int locks;

  /** The file locks, one entry per lock type, corresponding to the lock bytes. */
  private final FileLock[] fileLocks;

  /**
   * Creates a new {@link OffHeapWALIndex}. To determine if the index is modified while reading it
   * from memory, an extra copy of the index is stored adjacent to the first one. If these instances
   * are not equal, a concurrent modification is taking place at that time and creating a new {@link
   * OffHeapWALIndex} will fail with an exception.
   *
   * @param file The backing file of this WAL index.
   * @param memory The memory segment where the index must reside.
   * @throws ConcurrentModificationException If the WAL index is modified while reading it from
   *     shared memory.
   * @throws IOException If memory mapping any of the required file regions fails.
   */
  @SuppressWarnings("PMD.UseVarArgs") // headers has a fixed  length of 2
  @SuppressFBWarnings(
      value = "EI_EXPOSE_REP2",
      justification = "It makes no sense to copy or clone the mutable LuxorFile.")
  private OffHeapWALIndex(
      final LuxorFile file, final MemorySegment memory, final OffHeapWALIndexHeader... headers)
      throws IOException {
    if (headers.length != INDEX_HEADERS_SIZE) {
      throw new IllegalArgumentException("Expect exactly two headers");
    }

    if (headers[0].equals(headers[1])) {
      this.memory = memory;
      this.locks = WALLockType.NONE.mask();
      this.fileLocks = new FileLock[FILE_LOCK_COUNT];
      this.file = file;
      this.headers = headers;
      this.table = new OffHeapHashSet(file, LAYOUT.byteSize());
    } else {
      throw new ConcurrentModificationException(
          "Cannot create OffHeapWALIndex: concurrent modification detected.");
    }
  }

  /**
   * Loads two {@link OffHeapWALIndexHeader} instances from the given memory segment.
   *
   * @param memory The memory segment to load the index headers from.
   * @return The index headers.
   */
  private static OffHeapWALIndexHeader[] loadHeaders(final MemorySegment memory) {
    return new OffHeapWALIndexHeader[] {
      OffHeapWALIndexHeader.newBuilder(
              memory.asSlice(0L, OffHeapWALIndexHeader.LAYOUT).asReadOnly())
          .build(),
      OffHeapWALIndexHeader.newBuilder(
              memory
                  .asSlice(
                      LAYOUT.byteOffset(
                          MemoryLayout.PathElement.groupElement(HEADERS_NAME),
                          MemoryLayout.PathElement.sequenceElement(1L)),
                      OffHeapWALIndexHeader.LAYOUT)
                  .asReadOnly())
          .build()
    };
  }

  /**
   * Builds a new {@link OffHeapWALIndex}, using a database size of {@code dbSize}. The index will
   * be memory mapped into the first {@code LAYOUT.byteSize()} bytes of {@code shm}.
   *
   * @param dbSize The size of the database in pages.
   * @param randomSalt The random salt value, copied from the WAL header.
   * @param sequentialSalt The sequential salt value, copied from the WAL header.
   * @param shm The backing file for the index.
   * @return A new {@link OffHeapWALIndex} instance.
   * @throws IllegalArgumentException If {@code dbSize < 0}.
   * @throws NullPointerException If {@code shm} is {@code null}.
   * @throws IOException If memory-mapping the file fails.
   */
  @SuppressWarnings("java:S2245") // rng is not used in a security context.
  public static OffHeapWALIndex buildInitial(
      final long dbSize, final int randomSalt, final int sequentialSalt, final LuxorFile shm)
      throws IOException {
    ensureAtLeastZero(dbSize);
    final MemorySegment memory =
        requireNonNull(shm, "Cannot create OffHeapWALIndex: backing file must be non-null.")
            .mapShared(0L, LAYOUT.byteSize())
            .fill((byte) 0);

    final long checksum = new FNV1a().state();
    final long initialOffset = LAYOUT.byteOffset(HEADERS_ELEMENT);

    return new OffHeapWALIndex(
        shm,
        memory,
        OffHeapWALIndexHeader.newBuilder(
                memory.asSlice(initialOffset, OffHeapWALIndexHeader.LAYOUT))
            .dbSize(dbSize)
            .randomSalt(randomSalt)
            .sequentialSalt(sequentialSalt)
            .lastCommitFrame(-1)
            .cumulativeChecksum(checksum)
            .build(),
        OffHeapWALIndexHeader.newBuilder(
                memory.asSlice(
                    initialOffset + OffHeapWALIndexHeader.LAYOUT.byteSize(),
                    OffHeapWALIndexHeader.LAYOUT))
            .dbSize(dbSize)
            .randomSalt(randomSalt)
            .sequentialSalt(sequentialSalt)
            .lastCommitFrame(-1)
            .cumulativeChecksum(checksum)
            .build());
  }

  /**
   * Builds a new {@link OffHeapWALIndex}, based on the given {@code wal}, and it will be memory
   * mapped into the first {@code LAYOUT.byteSize()} bytes of {@code shm}.
   *
   * @param wal The write-ahead log to iterate.
   * @param shm The backing file for the index.
   * @return A new {@link OffHeapWALIndex} instance.
   * @throws ConcurrentModificationException If the WAL index is modified while reading it from
   *     shared memory, or if changes to the WAL contents are detected after obtaining the exclusive
   *     lock.
   * @throws CorruptWALException If the wal being read is corrupt or cannot be fully read.
   * @throws IOException If an I/O error occurs while reading the wal.
   */
  public static OffHeapWALIndex buildFromExisting(final LuxorFile wal, final LuxorFile shm)
      throws CorruptWALException, IOException {
    final MemorySegment memory =
        requireNonNull(shm, "Cannot build OffHeapWALIndex: backing file must be non-null.")
            .mapShared(0L, LAYOUT.byteSize());

    final OffHeapWALIndex walIndex = new OffHeapWALIndex(shm, memory, loadHeaders(memory));
    final OffHeapWALIndexHeader indexHeader = walIndex.header();

    // If the memory-mapped index header is empty, this means that the wal file was not mapped
    // previously, and we must fill it by iterating the WAL itself. To do so, we need a write lock.
    if (indexHeader.isEmpty() && walIndex.tryLockExclusiveOrdered()) {
      if (walIndex.isStale()) {
        walIndex.unlock();
        throw new ConcurrentModificationException(
            "WAL contents changed between empty-check and obtaining an exclusive lock.");
      }

      try {
        final LocalWALHeader walHeader = LocalWALHeader.readFromFile(wal, LocalWAL.HEADER_OFFSET);
        final LocalWALSpliterator spliterator = new LocalWALSpliterator(indexHeader, wal);
        StreamSupport.stream(spliterator, false)
            .filter(
                frame ->
                    frame.randomSalt() == walHeader.randomSalt()
                        && frame.sequentialSalt() == walHeader.sequentialSalt())
            .forEach(
                frame -> {
                  final int frameIndex = indexHeader.cursor();
                  walIndex.notifyAppended(frame, frameIndex);
                });
      } finally {
        walIndex.unlock();
      }
    }

    return walIndex;
  }

  /**
   * Returns whether a less restrictive lock than {@code requested} is already held.
   *
   * @param requested The requested lock type.
   * @return {@code true} if a less restrictive lock is held, {@code false} otherwise.
   */
  private boolean lockedLessRestrictiveThan(final WALLockType requested) {
    return this.locks < requested.mask();
  }

  /**
   * Returns whether a lock of the given type is currently held.
   *
   * @param type The lock type to check.
   * @return {@code true} if a lock of the given type is currently held, {@code false} otherwise.
   */
  private boolean hasLock(final WALLockType type) {
    return (this.locks & type.mask()) == type.mask();
  }

  /**
   * Obtain a shared lock on the WAL index.
   *
   * @throws LockFailedException If the lock cannot be obtained.
   */
  private void lockShared() throws LockFailedException {
    // Since we only get here if a less restrictive lock is held, we know for sure that a shared
    // lock is not held and therefore don't need to verify that before continuing.
    this.file.mutex().readLock().lock();

    try {
      final int position = WALLockType.SHARED.offset();
      this.fileLocks[position] = this.file.lock(position, 1L, true);
      this.locks |= WALLockType.SHARED.mask();
    } catch (IOException e) {
      this.file.mutex().readLock().unlock();
      throw new LockFailedException("Cannot obtain shared lock on WAL index file.", e);
    }
  }

  /**
   * Try to obtain a shared lock on the WAL index.
   *
   * @return {@code true} if the lock is now held by the current thread, {@code false} otherwise.
   */
  @SuppressWarnings(
      "PMD.CloseResource") // The FileLock must be stored until the index is unlocked later.
  private boolean tryLockShared() {
    boolean success = false;

    // Since we only get here if a less restrictive lock is held, we know for sure that a shared
    // lock is not held and therefore don't need to verify that before continuing.
    if (this.file.mutex().readLock().tryLock()) {
      try {
        final int position = WALLockType.SHARED.offset();
        final FileLock flock = this.file.tryLock(position, 1L, true);

        if (nonNull(flock)) {
          // We have the lock.
          this.fileLocks[position] = flock;
          this.locks |= WALLockType.SHARED.mask();
          success = true;
        } else {
          this.file.mutex().readLock().unlock();
        }
      } catch (IOException e) {
        this.file.mutex().readLock().unlock();
        log.trace("I/O error while trying to obtain a shared lock.", e);
      }
    }

    return success;
  }

  /** Release a shared lock on the WAL index. If no lock is held, this method has no effect. */
  private void unlockShared() {
    if (this.hasLock(WALLockType.SHARED)) {
      final int position = WALLockType.SHARED.offset();
      try {
        this.fileLocks[position].release();
      } catch (IOException e) {
        log.warn(
            "Could not unlock shared WAL index file lock, but must proceed as if it were unlocked.",
            e);
      }

      this.fileLocks[position] =
          null; // NOPMD (NullAssignment) undesirable fix is to use an ArrayList.
      this.file.mutex().readLock().unlock();
      this.locks ^= WALLockType.SHARED.mask();
    }
  }

  /**
   * Obtain an exclusive lock.
   *
   * @throws LockFailedException If the lock cannot be obtained.
   */
  private void lockExclusive() throws LockFailedException {

    // Since we only get here if a less restrictive lock is held, we know for sure that an exclusive
    // lock is not held and therefore don't need to verify that before continuing.
    //
    // To obtain an exclusive lock, a shared lock must also be held, because of three reasons:
    // 1. Before database rows can be updated, they must be read first.
    // 2. Unlike an exclusive lock, a shared lock prevents a checkpoint from being executed.
    // 3. The reason explained at the OutOfOrderLockException class documentation.
    if (!this.hasLock(WALLockType.SHARED)) {
      throw new OutOfOrderLockException(
          "Out of order locking: an exclusive lock is requested, but no shared lock is currently held.");
    }

    this.file.lock().lock();
    try {
      final int position = WALLockType.EXCLUSIVE.offset();
      this.fileLocks[position] = this.file.lock(position, 1L, false);
      this.locks |= WALLockType.EXCLUSIVE.mask();
    } catch (IOException e) {
      this.file.lock().unlock();

      throw new LockFailedException("Cannot obtain exclusive lock on WAL index file.", e);
    }
  }

  /**
   * Try to obtain an exclusive lock on the WAL index.
   *
   * @return {@code true} if the lock is now held by the current thread, {@code false} otherwise.
   * @throws OutOfOrderLockException If a shared lock is not held prior to calling this method.
   */
  @SuppressWarnings(
      "PMD.CloseResource") // The FileLock must be stored until the index is unlocked later.
  private boolean tryLockExclusive() throws OutOfOrderLockException {

    // Since we only get here if a less restrictive lock is held, we know for sure that an exclusive
    // lock is not held and therefore don't need to verify that before continuing.
    //
    // To obtain an exclusive lock, a shared lock must also be held, because of three reasons:
    // 1. Before database rows can be updated, they must be read first.
    // 2. Unlike an exclusive lock, a shared lock prevents a checkpoint from being executed.
    // 3. The reason explained at the OutOfOrderLockException class documentation.
    if (!this.hasLock(WALLockType.SHARED)) {
      throw new OutOfOrderLockException(
          "Out of order locking: an exclusive lock is requested, but no shared lock is currently held.");
    }

    boolean success = false;
    if (this.file.mutex().writeLock().tryLock()) {
      try {
        final int position = WALLockType.EXCLUSIVE.offset();
        final FileLock flock = this.file.tryLock(position, 1L, true);

        if (nonNull(flock)) {
          // We have the lock.
          this.fileLocks[position] = flock;
          this.locks |= WALLockType.EXCLUSIVE.mask();
          success = true;
        } else {
          this.file.mutex().writeLock().unlock();
        }
      } catch (IOException e) {
        this.file.mutex().writeLock().unlock();
        log.trace("I/O error while trying to obtain an exclusive lock.", e);
      }
    }

    return success;
  }

  /** Release an exclusive lock. If no exclusive lock is held, this method has no effect. */
  private void unlockExclusive() {
    if (this.hasLock(WALLockType.EXCLUSIVE)) {
      final int position = WALLockType.EXCLUSIVE.offset();
      try {
        this.fileLocks[position].release();
      } catch (IOException e) {
        log.warn(
            "Could not unlock exclusive WAL index file lock, but must proceed as if it were unlocked.",
            e);
      }

      this.fileLocks[position] =
          null; // NOPMD (NullAssignment) undesirable fix is to use an ArrayList.
      this.file.lock().unlock();
      this.locks ^= WALLockType.EXCLUSIVE.mask();
    }
  }

  /**
   * Tries to obtain an exclusive lock on this index. A shared lock is obtained first. If obtaining
   * the exclusive lock fails, all locks are released.
   *
   * @return {@code true} if the exclusive lock is held, {@code false} otherwise.
   */
  private boolean tryLockExclusiveOrdered() {
    boolean success = false;
    if (this.tryLockShared()) {
      try {
        if (this.tryLockExclusive()) {
          success = true;
        } else {
          this.unlock();
        }
      } catch (OutOfOrderLockException ignored) {
        // This cannot happen: we are the one holding the shared lock and this exception is only
        // thrown if no shared lock is held.
      }
    }

    return success;
  }

  /** {@inheritDoc} */
  @Override
  public OffHeapWALIndexHeader header() {
    return this.headers[0];
  }

  /** {@inheritDoc} */
  @Override
  public boolean isStale() {
    // Try to reload any pending changes into the off-heap memory segment.
    this.memory.load();

    final OffHeapWALIndexHeader[] shmHeaders = loadHeaders(this.memory);
    if (shmHeaders[0].equals(shmHeaders[1])) {
      return !this.headers[0].equals(shmHeaders[0]);
    } else {

      throw new ConcurrentModificationException(
          "Cannot verify OffHeapWALIndex changes: concurrent modification detected.");
    }
  }

  /** {@inheritDoc} */
  @Override
  public int findFrameIndexOf(final long pageIndex) {
    return this.table.keyOf(pageIndex);
  }

  /** {@inheritDoc} */
  @Override
  public void notifyAppended(final WALFrame frame, final int frameIndex) {
    this.headers[0].notifyAppended(frame, frameIndex);
    this.headers[1].notifyAppended(frame, frameIndex);
    this.table.put(frameIndex, frame.pageIndex());
  }

  /** {@inheritDoc} */
  @Override
  public void sync() {
    if (this.currentLock() != WALLockType.EXCLUSIVE) {
      log.warn("Synchronizing WAL index without exclusive lock.");
    }

    this.headers[0].sync();
    this.headers[1].sync();
  }

  /** {@inheritDoc} */
  @Override
  public WALLockType currentLock() {
    WALLockType currentLockType = WALLockType.NONE;

    if (this.hasLock(WALLockType.EXCLUSIVE)) {
      currentLockType = WALLockType.EXCLUSIVE;
    } else if (this.hasLock(WALLockType.SHARED)) {
      currentLockType = WALLockType.SHARED;
    }

    return currentLockType;
  }

  /** {@inheritDoc} */
  @Override
  public void lock(final WALLockType type) throws LockFailedException {
    requireNonNull(type, "requested lock type must be non-null.");

    if (this.lockedLessRestrictiveThan(type)) {
      if (type == WALLockType.SHARED) {
        this.lockShared();
      } else if (type == WALLockType.EXCLUSIVE) {
        this.lockExclusive();
      }
    }

    // We already have a more restrictive lock than the one being requested, so no action required.
  }

  /** {@inheritDoc} */
  @Override
  public boolean tryLock(final WALLockType type) throws OutOfOrderLockException {
    requireNonNull(type, "requested lock type must be non-null.");

    boolean success = true;

    if (this.lockedLessRestrictiveThan(type)) {
      if (type == WALLockType.SHARED) {
        success = this.tryLockShared();
      } else if (type == WALLockType.EXCLUSIVE) {
        success = this.tryLockExclusive();
      }
    }

    // We already have a more restrictive lock than the one being requested, so no action required.
    return success;
  }

  /** {@inheritDoc} */
  @Override
  public void unlock() {
    if (this.locks != WALLockType.NONE.mask()) {

      // If we're locked (as indicated by this.locks), then the shared lock is always held.
      // If the exclusive lock is held as well, unlock that first, since otherwise a checkpoint may
      // start before the exclusive lock has been released.
      this.unlockExclusive();
      this.unlockShared();
    }
  }

  /**
   * Reloads the header of this {@link OffHeapWALIndex} from memory.
   *
   * @throws ConcurrentModificationException If a concurrent modification to the index headers is
   *     detected
   */
  public void reload() {
    final OffHeapWALIndexHeader[] newHeaders = loadHeaders(this.memory);

    if (newHeaders[0].equals(newHeaders[1])) {
      this.headers[0] = newHeaders[0];
      this.headers[1] = newHeaders[1];
      this.table.reload();
    } else {
      throw new ConcurrentModificationException(
          "Cannot reload OffHeapWALIndex: concurrent modification detected.");
    }
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    this.unlock();
    this.file.close();
  }
}
