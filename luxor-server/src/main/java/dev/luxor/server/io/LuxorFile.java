package dev.luxor.server.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.FileLockInterruptionException;
import java.nio.channels.NonReadableChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link LuxorFile} is used to operate on database- and WAL files.
 *
 * <h3>Concurrency</h3>
 *
 * <h4>Synchronization</h4>
 *
 * <p>Instances of {@link LuxorFile} delegate their inter-thread synchronization to a unique file
 * serial. For example, on Unix systems, this serial is an inode. This ensures that even if files
 * are opened from different paths, synchronization will work throughout the JVM because all such
 * files share the same {@link FileSerial}.<br>
 * Although the {@link LuxorFile} is designed for concurrent accesses, it is <em>strongly</em>
 * recommended that every platform thread uses its own {@link LuxorFile} instance. This will provide
 * each thread with its own file descriptor, ensuring that the underlying {@link FileChannel} cannot
 * be used by other threads.
 *
 * <h4>Interrupts</h4>
 *
 * <p>Since {@link LuxorFile#read(ByteBuffer, long)}, {@link LuxorFile#writeLock()} and some other
 * methods are blocking until they either fail or achieve their goal, it is possible that the
 * current thread gets interrupted while executing those methods. When that happens, the {@link
 * FileChannel} that is used to manipulate the file will be closed and must be reopened to allow
 * further usage. Calling threads should catch the explicit exceptions thrown by these methods to
 * figure out if the file must be reopened, and call {@link LuxorFile#reopen()} to do so.
 *
 * @author houthacker
 */
public final class LuxorFile implements ReadWriteLock, AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(LuxorFile.class);

  /** The real, absolute path to the file. */
  private final Path path;

  /** The options that were used when opening the file. */
  private final OpenOption[] options;

  /** The unique serial of this file. */
  private final FileSerial serial;

  /** The channel for file manipulation. */
  private FileChannel channel;

  private LuxorFile(
      final Path path,
      final OpenOption[] options,
      final FileChannel channel,
      final FileSerial serial) {
    this.path = path;
    this.options = options;
    this.channel = channel;
    this.serial = serial;
  }

  /**
   * Opens a new {@link LuxorFile} using the given options.
   *
   * @param path The path to the file. May be relative.
   * @param options The options specifying how the file is opened.
   * @return A new {@link LuxorFile}.
   * @throws IOException If the given path cannot be resolved to a real path, the file cannot be
   *     opened using the given options or finally, if the unique file serial cannot be obtained.
   */
  public static LuxorFile open(final Path path, final OpenOption... options) throws IOException {
    final Path realPath = path.toRealPath();
    return new LuxorFile(
        realPath, options, FileChannel.open(realPath, options), FileSerial.find(realPath));
  }

  /**
   * Tells whether this file is open.<br>
   * If the file is not open, it cannot be used anymore and calling threads must obtain a new {@link
   * LuxorFile} instance.
   *
   * @return {@code true} if, and only if, this file is open.
   */
  public boolean isOpen() {
    return this.channel.isOpen();
  }

  /**
   * Reopens the file if it has been closed. If the file has not been closed, this method has no
   * effect. Use this method with caution.
   *
   * @throws IOException If the file cannot be reopened.
   */
  public void reopen() throws IOException {
    if (!this.channel.isOpen()) {
      log.debug("Attempting to reopen the channel for LuxorFile(path={})", path);
      this.channel = FileChannel.open(this.path, this.options);
    }
  }

  /**
   * Closes this file.
   *
   * @throws IOException If an I/O error occurs.
   */
  @Override
  public void close() throws IOException {
    this.channel.close();
  }

  /**
   * Reads {@link ByteBuffer#limit()} bytes into {@code destination}, starting at file offset {@code
   * offset}.
   *
   * @param destination The buffer to read the bytes into.
   * @param offset The file offset to start reading at.
   * @return The amount of bytes read, or -1 if the underlying {@link FileChannel} has reached
   *     end-of-stream.
   * @throws IllegalArgumentException If the offset is negative or the buffer is read-only.
   * @throws NonReadableChannelException If the underlying {@link FileChannel} was not opened for
   *     reading.
   * @throws ClosedChannelException If the underlying {@link FileChannel} is closed.
   * @throws AsynchronousCloseException If another thread closes the underlying {@link FileChannel}
   *     while the read operation is in progress.
   * @throws ClosedByInterruptException If another thread interrupts the current thread while the
   *     read operation is in progress, thereby closing the channel and setting the current thread's
   *     interrupt status.
   * @throws IOException If some other I/O error occurs.
   */
  public int read(final ByteBuffer destination, final long offset) throws IOException {
    return this.channel.read(destination, offset);
  }

  /**
   * Writes all bytes from the given buffer to this file, starting at file offset {@code offset}.
   *
   * @param source The buffer to write to the file.
   * @param offset The file offset to start writing at.
   * @return The amount of bytes actually written, which may be zero.
   * @throws IllegalArgumentException If the offset is negative.
   * @throws NonWritableChannelException If this file was not opened for writing.
   * @throws ClosedChannelException If the underlying {@link FileChannel} is closed.
   * @throws AsynchronousCloseException If another thread closes the underlying {@link FileChannel}
   *     while the write operation is in progress.
   * @throws ClosedByInterruptException If another thread interrupts the current thread while the
   *     write operation is in progress, thereby closing the channel and setting the current
   *     thread's interrupt status.
   * @throws IOException If some other I/O error occurs.
   */
  public int write(final ByteBuffer source, final long offset) throws IOException {
    return this.channel.write(source, offset);
  }

  /**
   * Forces any updates to this file to be written to the storage that contains it.
   *
   * @throws ClosedChannelException If the underlying {@link FileChannel} is closed.
   * @throws IOException If some other I/O error occurs.
   * @see FileChannel#force(boolean)
   */
  public void sync() throws IOException {
    this.channel.force(false);
  }

  /** {@inheritDoc} */
  @Override
  public Lock readLock() {
    return this.serial.readLock();
  }

  /** {@inheritDoc} */
  @Override
  public Lock writeLock() {
    return this.serial.writeLock();
  }

  /**
   * Acquires a lock on the given region of this file. An extended description of the implementation
   * can be found at {@link FileChannel#lock(long, long, boolean)}.
   *
   * @param position The position at which the locked region is to start; must be non-negative.
   * @param size The size of the locked region; must be non-negative, and the sum {@code position +
   *     size} must be non-negative. A value of zero means to lock all bytes from the specified
   *     starting position to the end of the file, regardless of whether the file is subsequently
   *     extended or truncated.
   * @param shared {@code true} to request a shared lock, in which case this file must be open for
   *     reading (and possibly writing); {@code false} to request an exclusive lock, in which case
   *     this file must be open for writing (and possibly reading).
   * @return A lock object representing the newly-acquired lock.
   * @throws IllegalArgumentException If the preconditions on the parameters do not hold.
   * @throws ClosedChannelException If the underlying {@link FileChannel} is closed.
   * @throws AsynchronousCloseException If another thread closes this channel while the invoking
   *     thread is blocked in this method.
   * @throws FileLockInterruptionException If the invoking thread is interrupted while blocked in
   *     this method.
   * @throws OverlappingFileLockException If a lock that overlaps the requested region is already
   *     held by this Java virtual machine, or if another thread is already blocked in this method
   *     and is attempting to lock an overlapping region.
   * @throws NonReadableChannelException If {@code shared} is {@code true} but this file was not
   *     opened for reading.
   * @throws NonWritableChannelException If {@code shared} is {@code false} but this file was not
   *     opened for writing.
   * @throws IOException If some other I/O error occurs.
   * @see #tryLock(long,long,boolean)
   */
  public FileLock lock(final long position, final long size, final boolean shared)
      throws IOException {
    return this.channel.lock(position, size, shared);
  }

  /**
   * Attempts to acquire a lock on the given region of this file. If the lock cannot be acquired,
   * this method returns immediately. An extended description of the implementation can be found at
   * {@link FileChannel#tryLock(long, long, boolean)}.
   *
   * @param position The position at which the locked region is to start; must be non-negative.
   * @param size The size of the locked region; must be non-negative, and the sum {@code position +
   *     size} must be non-negative. A value of zero means to lock all bytes from the specified
   *     starting position to the end of the file, regardless of whether the file is subsequently
   *     extended or truncated.
   * @param shared {@code true} to request a shared lock, {@code false} to request an exclusive
   *     lock.
   * @return A lock object representing the newly-acquired lock, or {@code null} if the lock could
   *     not be acquired because another program holds an overlapping lock.
   * @throws IllegalArgumentException If the preconditions on the parameters do not hold.
   * @throws ClosedChannelException If the underlying {@link FileChannel} is closed.
   * @throws OverlappingFileLockException If a lock that overlaps the requested region is already
   *     held by this Java virtual machine, or if another thread is already blocked in this method
   *     and is attempting to lock an overlapping region of the same file.
   * @throws NonReadableChannelException If {@code shared} is {@code true} but this channel was not
   *     opened for reading.
   * @throws NonWritableChannelException If {@code shared} is {@code false} but this channel was not
   *     opened for writing.
   * @throws IOException If some other I/O error occurs.
   * @see #lock(long,long,boolean)
   */
  public FileLock tryLock(final long position, final long size, final boolean shared)
      throws IOException {
    return this.channel.tryLock(position, size, shared);
  }
}