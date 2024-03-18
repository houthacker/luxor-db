package dev.luxor.server.wal;

import dev.luxor.server.concurrent.LockFailedException;
import dev.luxor.server.concurrent.OutOfOrderLockException;
import dev.luxor.server.io.CorruptPageException;
import dev.luxor.server.io.NoSuchPageException;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ConcurrentModificationException;

/**
 * Interface for all {@code luxor} write-ahead-log (WAL) implementations.
 *
 * <h4>Design</h4>
 *
 * <p>The write-ahead-log (WAL) delivers an important part of the ACID characteristics {@code luxor}
 * provides:
 *
 * <h5>Atomicity</h5>
 *
 * <p>The {@link WriteAheadLog} enables {@code luxor} to treat transactions as a single 'unit': they
 * either completely succeed, or completely fail. This is achieved by two cooperating principles:
 * grouping related page writes in a transaction (atomicity) and the visibility of these updated
 * pages to other clients (isolation).
 *
 * <h5>Durability</h5>
 *
 * <p>Clients writing new or updated pages will always append them to the end of WAL. If a client
 * then commits the transaction, a special WAL frame is written indicating the transaction is
 * committed.<br>
 * If the transaction is in synchronous mode, writing this type of frame causes the WAL contents to
 * be synchronized if the implementation supports it. This ensures the written data now is persisted
 * on the underlying storage and will survive events like a system crash and power outages. Not only
 * does this guarantee durability, but also atomicity: if the {@code sync()} fails, the whole
 * transaction is considered failed and should be rolled back.<br>
 * If the transaction is in asynchronous mode, appending a commit frame may happen out-of-order and
 * therefore does not cause the WAL contents to be synchronized to storage. Instead, if the
 * implementation supports it, this is done when a checkpoint finishes. In other words: creating a
 * database in asynchronous mode will allow more throughput and more efficient use of CPU and disk
 * resources at the cost of a lower durability (a higher risk of data loss).
 *
 * <p>The default WAL implementation ({@link LocalWAL}) used by {@code luxor} supports synchronizing
 * WAL contents to storage, but other implementations might not. Whether these implementations
 * support synchronization to persistent storage must be documented by these implementations.
 *
 * <h5>Isolation</h5>
 *
 * <p>Clients that read pages always first check the WAL whether the page they are looking for
 * possibly is stored there. To ensure these clients do not 'see' uncommitted transactions, but at
 * the same time providing them with a consistent view of the database for the duration of their
 * (read-)transaction, a <em>read-mark</em> is set when a read-transaction starts. This mark is the
 * WAL-frame number of the last valid commit frame within the WAL. Since a read-mark of zero
 * indicates the WAL contains no valid, committed pages, clients will not use the WAL in that case,
 * but can safely read directly from the database file.
 *
 * <p>In fact, the last valid commit frame always contains the most recent root page of the
 * underlying B+Tree structure that is used to store and retrieve pages of the database. This is
 * because {@code luxor} uses an append-only B+Tree to store its data. In this tree, new pages are
 * always appended at the end in such a manner that the root page is the last page of the file.
 * Although pages can be written to the WAL out of order, it is enforced that new roots are always
 * appended last, and therefore the last valid commit frame always contains the latest B+Tree root
 * page.
 *
 * <h5>Performance</h5>
 *
 * <p>One way to find pages within the WAL would be to just traverse the complete WAL file. Although
 * this will work, having some code that can quickly determine if a page is currently in the WAL
 * without reading the complete WAL will be faster. This is why the {@link WALIndex} exists. The
 * {@link WALIndex} can answer the following question quickly: <em>given a maximum WAL frame M,
 * which frame contains a page P?</em>.
 *
 * <h4>Structure</h4>
 *
 * <p>The WAL consists of a header at the beginning of the file, followed by a list of wal frames.
 * When the WAL is written to for the first time, it allocates space for 1000 frames, which is
 * roughly 4 Mb given a page size of 4096 bytes. The WAL file also grows in increments of 1000
 * frames.
 *
 * <h4>Concurrency</h4>
 *
 * <p>It is <em>strongly</em> advised to use a single {@link WriteAheadLog} instance per thread. The
 * concurrency implementation of {@code luxor} assumes, but not enforces this. Not using exactly one
 * unique instance per thread will almost certainly lead to data corruption.
 *
 * @author houthacker
 */
@SuppressWarnings("PMD.CommentSize")
public interface WriteAheadLog extends Closeable {

  /**
   * Reads the WAL header from the backing storage.
   *
   * <p>The header of this WAL keeps track of changes to the WAL file. In case of a system failure,
   * this header is used to recover and clean up the WAL file, removing any stale data from
   * uncommitted transactions.
   *
   * @return The WAL header.
   * @throws IOException If an I/O error occurs while reading the header.
   * @throws CorruptWALException If the WAL is corrupted.
   */
  WALHeader header() throws IOException, CorruptWALException;

  /**
   * Begins a new read transaction. If the calling thread already is in a transaction (read or
   * write) at the time this method is called, this method has no additional effect and therefore
   * uses the same database snapshot as the other running transaction(s) created by the calling
   * thread.
   *
   * <p>On success this method acquires a read lock on the WAL.
   *
   * @throws ConcurrentModificationException If a concurrent modification to the index headers is
   *     detected while attempting to start the read transaction.
   * @throws LockFailedException If the associated shared lock cannot be acquired.
   */
  void beginReadTransaction() throws LockFailedException;

  /**
   * Releases the shared lock held by the calling thread, ending the transaction. If no transaction
   * is running, this method has no effect.
   */
  void endReadTransaction();

  /**
   * Returns the frame number in which {@code pageNumber} is stored, or {@code -1} if no such frame
   * exists.
   *
   * @param pageNumber The page number to search for.
   * @return The frame index.
   */
  int frameIndexOf(long pageNumber);

  /**
   * Reads the page at {@code frameIndex}.
   *
   * @param frameIndex The frame index to read.
   * @return A {@link ByteBuffer} containing the page content.
   * @throws CorruptPageException If the page cannot be read completely.
   * @throws IOException If an I/O error occurs while reading the page.
   * @throws NoSuchPageException If the file contains no frame with {@code frameIndex}.
   */
  ByteBuffer pageAt(int frameIndex) throws IOException, NoSuchPageException;

  /**
   * Begins a new write transaction. A shared lock must already be held prior to this; this should
   * be done by starting a read transaction first. If the calling thread already is in a write
   * transaction at the time this method is called, this method has no additional effect and
   * therefore uses the same database snapshot as the other running transaction(s) created by the
   * calling thread.
   *
   * <p>If the WAL contents have changed since the related read transaction has started, the write
   * transaction cannot begin. Clients should call {{@link #endReadTransaction()}} and retry.
   *
   * <p>On success this method acquires a write lock on the WAL.
   *
   * @throws ConcurrentModificationException If a concurrent modification to the index headers is
   *     detected while attempting to start the read transaction.
   * @throws StaleWALException If the WAL contents changed since the related read transaction was
   *     started.
   * @throws OutOfOrderLockException If the calling thread does not hold a shared lock prior to
   *     calling this method.
   * @throws LockFailedException If the associated exclusive lock cannot be acquired.
   */
  void beginWriteTransaction() throws LockFailedException, StaleWALException;

  /**
   * Releases the exclusive- and shared lock held by the calling thread, ending the transaction. If
   * no transaction is running, this method has no effect.
   */
  void endWriteTransaction();
}
