package dev.luxor.server.io;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import dev.luxor.server.concurrent.Locks;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link FileSerial} is a unique identification of a given file or directory within its
 * filesystem, and should be used to synchronize access between threads to ths underlying resource.
 *
 * @author houthacker
 */
public final class FileSerial implements ReadWriteLock {

  private static final Logger log = LoggerFactory.getLogger(FileSerial.class);

  /**
   * The linked list of all FileSerials. The list is always searched backwards, because the last
   * FileSerial often has the highest change of reuse.
   */
  private static FileSerial tail;

  /**
   * The global lock which must be used to execute structural changes on {@link FileSerial#tail}.
   */
  private static final ReentrantReadWriteLock GLOBAL_SERIAL_GUARD = new ReentrantReadWriteLock();

  /** The unique identifying object of this {@code FileSerial}. */
  private final Object key;

  /** The r/w lock to (un)lock this {@code FileSerial}. */
  private final ReentrantReadWriteLock guard;

  /** The amount of objects (in the current JVM) that refer to this {@code FileSerial}. */
  private final AtomicInteger refCount;

  /** The next {@code FileSerial}, if any. */
  private FileSerial next;

  /** The previous {@code FileSerial}, if any. */
  private FileSerial previous;

  private FileSerial(final Object key) {
    this.key = key;
    this.guard = new ReentrantReadWriteLock();
    this.refCount = new AtomicInteger(1);
  }

  /** Resets the global linked list of referenced {@link FileSerial} instances. */
  private static void resetGlobalSerials() {
    tail = null; // NOPMD tail must be assigned to null here to indicate the list is empty.
  }

  /**
   * Acquires a unique identification of the given path. Multiple paths leading to the same location
   * will yield the same {@link FileSerial}.
   *
   * @implNote All {@link FileSerial} instances are stored in a linked list to enable finding them
   *     later, ensuring the same instance is used for paths resolving to the same unique serial.
   *     <br>
   *     Access to this linked list is synchronized, so if any structural changes must be made to
   *     this linked list (for example inserting a new {@link FileSerial} or if another serial is
   *     being removed by another thread), this method blocks on those operations before returning
   *     the {@link FileSerial}.
   * @param path The path to identify.
   * @return The serial of the path.
   * @throws IOException If an I/O error occurs while reading the file attributes, or if the path
   *     cannot be resolved to an absolute path.
   */
  public static FileSerial find(final Path path) throws IOException {
    final BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes.class);

    Object key = attrs.fileKey();
    if (isNull(key)) {
      log.debug(
          "The filesystem containing file at {} provides no unique file serial; using real path name as fallback",
          path);
      key = path.toRealPath().toString();
    }

    FileSerial serial = tail;

    // Prevent structural changes in the linked list while traversing it.
    GLOBAL_SERIAL_GUARD.readLock().lock();
    try {
      while (nonNull(serial)) {
        if (serial.key.equals(key)) {
          serial.refCount.incrementAndGet();

          return serial; // NOPMD require early exit.
        }

        serial = serial.previous;
      }
    } finally {
      GLOBAL_SERIAL_GUARD.readLock().unlock();
    }

    log.trace("FileSerial {} of path {} not yet referenced; creating new reference.", key, path);
    serial = new FileSerial(key);
    serial.previous = tail;

    GLOBAL_SERIAL_GUARD.writeLock().lock();
    try {

      // If the tail is not null, move it before serial.
      if (nonNull(tail)) {
        final Lock lock = tail.writeLock();
        lock.lock();
        try {
          tail.next = serial;
        } finally {
          lock.unlock();
        }
      }

      // Finally set the new tail to serial.
      tail = serial;
    } finally {
      GLOBAL_SERIAL_GUARD.writeLock().unlock();
    }

    return serial;
  }

  /**
   * Decrements the reference count of this {@link FileSerial} and removes it from the global linked
   * list if it has no more references. The latter is a blocking operation for calling threads.<br>
   * This method should typically be used
   */
  public void dereference() {
    // If decreasing the reference count yields zero, remove the FileSerial from the linked list.
    if (this.referenceCount() > 0 && this.refCount.decrementAndGet() == 0) {
      log.trace(
          "FileSerial {} has no more references; removing it from the linked list.", this.key);

      // Removing the serial is a structural change, acquire the write lock.
      GLOBAL_SERIAL_GUARD.writeLock().lock();
      try {
        if (nonNull(this.previous) && nonNull(this.next)) {

          Locks.exclusiveLock(this.previous.guard, this.next.guard);
          try {
            this.previous.next = this.next;
            this.next.previous = this.previous;
          } finally {
            Locks.exclusiveUnlock(this.previous.guard, this.next.guard);
          }
        } else if (nonNull(this.previous)) {
          final Lock lock = this.previous.writeLock();
          lock.lock();
          try {
            this.previous.next = this.next;
          } finally {
            lock.unlock();
          }
        } else if (nonNull(this.next)) {
          final Lock lock = this.next.writeLock();
          lock.lock();
          try {
            this.next.previous = this.previous;
          } finally {
            lock.unlock();
          }
        }

        // If this is the last inode, reset the tail pointer.
        if (this.equals(tail)) {
          resetGlobalSerials();
        }

      } finally {
        GLOBAL_SERIAL_GUARD.writeLock().unlock();
      }
    }
  }

  /**
   * Returns the lock that must be owned when shared access to this serial is required.
   *
   * @return The shared lock.
   */
  @Override
  public Lock readLock() {
    return this.guard.readLock();
  }

  /**
   * Returns the lock that must be owned when exclusive access to this serial is required.
   *
   * @return The unique lock.
   */
  @Override
  public Lock writeLock() {
    return this.guard.writeLock();
  }

  /**
   * Returns the amount of files referencing this {@link FileSerial} in the current JVM instance.
   *
   * @return The amount of references.
   */
  public int referenceCount() {
    return this.refCount.get();
  }

  @Override
  public String toString() {
    return String.format("FileSerial(refs=%d,serial=[%s])", this.referenceCount(), this.key);
  }
}
