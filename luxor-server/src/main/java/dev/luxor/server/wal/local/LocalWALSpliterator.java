package dev.luxor.server.wal.local;

import static java.util.Objects.requireNonNull;

import dev.luxor.server.io.LuxorFile;
import dev.luxor.server.io.Page;
import dev.luxor.server.wal.CorruptWALException;
import dev.luxor.server.wal.WALFrame;
import dev.luxor.server.wal.WALSpliterator;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NonReadableChannelException;
import java.util.Spliterator;
import java.util.function.Consumer;

/**
 * A {@link WALSpliterator} implementation that traverses over a {@link LocalWAL} given the {@link
 * dev.luxor.server.io.LuxorFile} that contains it. This spliterator has the following {@link
 * #characteristics()}:
 *
 * <ul>
 *   <li>{@link Spliterator#ORDERED}
 *   <li>{@link Spliterator#SIZED}
 *   <li>{@link Spliterator#NONNULL}
 *   <li>{@link Spliterator#IMMUTABLE}
 *   <li>{@link Spliterator#SUBSIZED}
 * </ul>
 *
 * @implNote This class is {@code final} to prevent finalizer attacks.
 * @author houthacker
 */
public final class LocalWALSpliterator implements WALSpliterator {

  /** The characteristics of this {@link LocalWALSpliterator}. */
  private static final int CHARACTERISTICS = ORDERED | SIZED | NONNULL | IMMUTABLE | SUBSIZED;

  /** The WAL file we're traversing. */
  private final LuxorFile wal;

  /** The total amount of frames in the write-ahead log. */
  private final int frameCount;

  /** The index of the next frame to read. */
  private int cursor;

  /**
   * Creates a new {@link LocalWALSpliterator} using the given backing {@code wal}.
   *
   * @param indexHeader The header of the {@link OffHeapWALIndex}
   * @param wal The write-ahead log to use.
   * @throws CorruptWALException If reading the WAL fails due to a corrupt wal.
   * @throws IOException If an I/O error occurs when reading the wal.
   */
  public LocalWALSpliterator(final OffHeapWALIndexHeader indexHeader, final LuxorFile wal)
      throws CorruptWALException, IOException {
    this.wal = requireNonNull(wal, "wal must be non-null.");

    final long walFileSize = wal.size();
    if (walFileSize < LocalWALHeader.BYTES) {
      throw new CorruptWALException("WAL file contains an invalid or incomplete header.");
    }

    this.frameCount = calculateFrameCount(walFileSize, indexHeader);
    this.cursor = 0;
  }

  /**
   * Calculates the frame count of the write-ahead log.
   *
   * <p>The size is calculated based on the last commit frame index if it is not {@code 0}. If it is
   * {@code 0}, the size will be calculated using the file size, ignoring the last frame if it is
   * not fully written yet.
   *
   * @param walFileSize The WAL file size in bytes.
   * @param indexHeader The index header as it is read from the WAL.
   * @return The amount of frames to read from the WAL.
   */
  private static int calculateFrameCount(
      final long walFileSize, final OffHeapWALIndexHeader indexHeader) {
    int frameCount = indexHeader.lastCommitFrame();
    if (indexHeader.lastCommitFrame() == 0) {
      // WAL size is unknown or empty, try and calculate from wal file size.
      final int base = (int) walFileSize - LocalWALHeader.BYTES;
      final int remainder = base % WALFrame.BYTES;

      // It is possible that the WAL file is currently being written to, and therefore contains an
      // incomplete frame. In that case, just ignore that frame.
      // If there is an incomplete frame and a write transaction is held by another thread or
      // process, this should lead to a CorruptWALException but that is not the responsibility of
      // this spliterator.
      frameCount = (base - remainder) / WALFrame.BYTES;
    }

    return frameCount;
  }

  /**
   * Reads the next {@link WALFrame} and supplies it to {@code action}.
   *
   * @apiNote The {@link UncheckedIOException} can be thrown with the following exceptions as its
   *     cause:
   *     <ul>
   *       <li>{@link ClosedByInterruptException} If the WAL is closed by an interrupt.
   *       <li>{@link AsynchronousCloseException} If another thread closed the WAL.
   *       <li>{@link ClosedChannelException} If the WAL is closed.
   *       <li>{@link IOException} If an I/O error occurs while reading a wal frame.
   *     </ul>
   *
   * @param action The action whose operation is performed at-most once
   * @return {@code false} if no remaining elements exist upon entry to this method, {@code true}
   *     otherwise.
   * @throws NullPointerException If {@code action} is {@code null}.
   * @throws NonReadableChannelException If the underlying {@link java.nio.channels.FileChannel} is
   *     not open for reading.
   * @throws UncheckedIOException If an I/O error occurs when trying to read the next WAL frame.
   */
  @Override
  @SuppressWarnings("PMD.OnlyOneReturn") // Allow for early exits.
  public boolean tryAdvance(final Consumer<? super WALFrame> action) {
    requireNonNull(action, "action must be non-null.");
    if (this.cursor + 1 == this.frameCount) {
      return false;
    }

    final ByteBuffer frameBuffer = ByteBuffer.allocate(WALFrame.BYTES);
    try {
      final int bytesRead =
          this.wal.read(frameBuffer, LocalWALHeader.BYTES + (long) this.cursor * WALFrame.BYTES);
      this.cursor++;
      if (bytesRead == WALFrame.BYTES) {
        frameBuffer.rewind();

        action.accept(
            WALFrame.newBuilder()
                .header(frameBuffer.slice(0, WALFrame.HEADER_BYTES))
                .page(frameBuffer.slice(WALFrame.HEADER_BYTES, Page.BYTES))
                .build());
      }

    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read from WAL.", e);
    }

    return true;
  }

  /** {@inheritDoc} */
  @Override
  public Spliterator<WALFrame> trySplit() {
    return null;
  }

  /** {@inheritDoc} */
  @Override
  public long estimateSize() {
    return this.frameCount;
  }

  /** {@inheritDoc} */
  @Override
  public int characteristics() {
    return CHARACTERISTICS;
  }
}
