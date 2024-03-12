package dev.luxor.server.wal;

import static dev.luxor.server.shared.Ensure.ensureAtLeastZero;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;

import dev.luxor.server.io.LuxorFile;
import java.io.IOException;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A minimal HashSet implementation with off-heap backed storage, using open addressing.
 *
 * <h4>Supported key/value types</h4>
 *
 * <p>It supports only primitive types as its key({@code int}) and value ({@code long}).<br>
 * An {@link OffHeapHashSet} operates on an on-heap copy of the off-heap data.
 *
 * <h4>Concurrency</h4>
 *
 * <p>A {@link OffHeapHashSet} is not synchronized, since it is not meant to be shared by multiple
 * threads. However, its off-heap backing storage <em>is</em> meant to be used by multiple processes
 * and threads. This implementation assumes, but not enforces, that proper concurrency control is
 * imposed before calling any of the methods that might structurally change the contents of this
 * {@link OffHeapHashSet}.
 *
 * @author houthacker
 */
public class OffHeapHashSet implements WALIndexTable {

  private static final Logger log = LoggerFactory.getLogger(OffHeapHashSet.class);

  private static final String ENTRY_KEY_NAME = "key";
  private static final String ENTRY_VALUE_NAME = "value";

  private static final MemoryLayout ENTRY_LAYOUT =
      MemoryLayout.structLayout(
          ValueLayout.JAVA_INT.withName(ENTRY_KEY_NAME),
          MemoryLayout.paddingLayout(ValueLayout.JAVA_INT.byteSize()),
          ValueLayout.JAVA_LONG.withName(ENTRY_VALUE_NAME));

  private static final String HASH_SET_CAPACITY_NAME = "capacity";
  private static final String HASH_SET_SIZE_NAME = "size";
  private static final String HASH_SET_ENTRIES_NAME = "entries";

  public static final MemoryLayout LAYOUT =
      MemoryLayout.structLayout(
              ValueLayout.JAVA_INT.withName(HASH_SET_CAPACITY_NAME),
              ValueLayout.JAVA_INT.withName(HASH_SET_SIZE_NAME),
              ValueLayout.ADDRESS.withTargetLayout(ENTRY_LAYOUT).withName(HASH_SET_ENTRIES_NAME))
          .withName("hash_set");

  /**
   * When an {@link OffHeapHashSet} becomes fairly large (roughly 10Mb), this fact is logged to the
   * appropriate output. Because the contents of the memory-mapped off-heap area are copied into an
   * {@link OffHeapHashSet}, having a fairly large set can quickly saturate the available JVM
   * allotted memory, since each client connection (carrier thread) has its own {@link
   * OffHeapHashSet} to store the WAL index in.
   */
  private static final int SIZE_WARNING_THRESHOLD = 655360;

  /**
   * If an element is inserted while the load factor is at least this value, the hash set is grown.
   */
  private static final double MAX_LOAD_FACTOR = 0.75d;

  /** The initial capacity of this {@link OffHeapHashSet} if the off-heap capacity is 0. */
  private static final int INITIAL_CAPACITY = 4096;

  /** The backing file. */
  private final LuxorFile file;

  /** The file offset in bytes of the entries address in off-heap memory. */
  private final long offset;

  /** The storage of the {@code hash_set}. */
  private final MemorySegment memory;

  private ByteBuffer entries;

  /**
   * The maximum amount of entries that can be stored in this hash set without allocating more
   * memory.
   */
  private int capacity;

  /** The amount of entries residing in this hash set. */
  private int size;

  /**
   * Creates a new {@link OffHeapHashSet} using the provided {@code file} to allocate new space when
   * required.
   *
   * @param file The memory-mapped file.
   * @param offset The offset in the memory-mapped file for the {@link MemorySegment} of this
   *     HashSet.
   */
  public OffHeapHashSet(final LuxorFile file, final long offset) throws IOException {
    this.file = requireNonNull(file, "file");
    this.offset = ensureAtLeastZero(offset);
    this.memory = this.file.mapShared(offset, LAYOUT.byteSize());

    this.load();
  }

  /**
   * Fills {@code buf} with empty entries, returning the same buffer after rewinding it.
   *
   * @param buf The buffer to fill.
   * @return The filled buffer.
   */
  private static ByteBuffer fillWithEmptyEntries(final ByteBuffer buf) {
    buf.rewind();

    while (buf.hasRemaining()) {
      buf.putLong(0xffffffff00000000L); // int(-1) and padding
      buf.putLong(-1);
    }

    return buf.rewind();
  }

  /**
   * Returns the entry at the given {@code index}.
   *
   * @param index The index at which the entry resides.
   * @return The entry, or {@code null} if no entry resides at the given index.
   */
  private Entry getEntryAt(final int index) {
    this.entries.position((int) (index * ENTRY_LAYOUT.byteSize()));

    final int key = (int) (this.entries.getLong() >>> 32);
    final long value = this.entries.getLong();

    if (key == -1) {
      return null;
    }

    return new Entry(key, value);
  }

  /**
   * Sets the entry at the given {@code index}.
   *
   * @param index The index at which to set the entry.
   * @param entry The entry to set.
   */
  private void setEntryAt(final int index, final Entry entry) {
    this.entries.position((int) (index * ENTRY_LAYOUT.byteSize()));

    this.entries.putLong((long) entry.key << 32).putLong(entry.value);
  }

  /**
   * Calculates the current load factor.
   *
   * @return The current load factor.
   */
  private double loadFactor() {
    return this.size / (this.entries.rewind().limit() / (ENTRY_LAYOUT.byteSize() * 1.0d));
  }

  /**
   * Grows this {@link OffHeapHashSet} by doubling its size. After allocating the memory, the data
   * are re-hashed.
   *
   * @throws ArithmeticException if growing the capacity would cause an integer overflow.
   */
  private void grow() {
    final int newCapacity = this.capacity << 1;
    if (newCapacity < 0) {
      throw new ArithmeticException("Integer overflow.");
    }

    final ByteBuffer oldBuffer = this.entries;
    this.entries =
        fillWithEmptyEntries(ByteBuffer.allocate((int) (newCapacity * ENTRY_LAYOUT.byteSize())));

    this.size = 0;
    this.capacity = newCapacity;
    while (oldBuffer.hasRemaining()) {
      final int key = (int) (oldBuffer.getLong() >>> 32);
      final long value = oldBuffer.getLong();

      if (key != -1) {
        this.put(key, value);
      }
    }
  }

  /**
   * Calculates the bucket for the given {@code key} and {@code value} must be stored in. Although
   * this method walks all entries until it found an empty slot, this is guaranteed to terminate if
   * {@link OffHeapHashSet#MAX_LOAD_FACTOR} is {@code < 1.0}.
   *
   * @param key The entry key.
   * @param value The entry value.
   * @return The bucket to store the entry in.
   */
  private int bucketOf(final int key, final long value) {
    int hash = Long.hashCode(value);

    int bucket = hash % this.capacity;
    Entry entry;
    while (nonNull(entry = this.getEntryAt(bucket))) {
      if (entry.key == key) {
        return bucket;
      }

      bucket++;
      bucket %= this.capacity;
    }

    return bucket;
  }

  /** Copies the memory-mapped entries from off-heap memory into this {@link OffHeapHashSet}. */
  public final void load() {
    // Read capacity from memory, allocate first block heap-memory if cap == 0.
    final int cap =
        this.memory.get(
            ValueLayout.JAVA_INT,
            LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement(HASH_SET_CAPACITY_NAME)));
    this.capacity = cap == 0 ? INITIAL_CAPACITY : cap;

    this.size =
        this.memory.get(
            ValueLayout.JAVA_INT,
            LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement(HASH_SET_SIZE_NAME)));

    if (this.size >= SIZE_WARNING_THRESHOLD) {
      log.warn(
          "Risk of fast JVM heap saturation: loading {} entries (+/- {} bytes) into OffHeapHashSet.",
          this.size,
          this.size * ENTRY_LAYOUT.byteSize());
    }

    if (cap > 0) {
      final MemorySegment ms =
          this.memory.get(
              ValueLayout.ADDRESS,
              LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement(HASH_SET_ENTRIES_NAME)));
      final MemorySegment ptr = ms.reinterpret(this.capacity * ENTRY_LAYOUT.byteSize());
      this.entries =
          MemorySegment.ofArray(new byte[(int) ptr.byteSize()]).copyFrom(ptr).asByteBuffer();

    } else {

      this.entries =
          fillWithEmptyEntries(
              ByteBuffer.allocate((int) (this.capacity * ENTRY_LAYOUT.byteSize())));
    }
  }

  /**
   * Flushes the contents of this {@link OffHeapHashSet} into the off-heap memory-mapped backing
   * storage.
   *
   * @throws ArithmeticException If the required memory allocation size overflows {@link
   *     Integer#MAX_VALUE}.
   */
  public void flush() throws IOException {
    final int cap =
        this.memory.get(
            ValueLayout.JAVA_INT,
            LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement(HASH_SET_CAPACITY_NAME)));

    MemorySegment offHeap;
    if (cap < this.capacity) {
      offHeap =
          this.file.mapShared(
              this.offset + LAYOUT.byteSize(), this.capacity * ENTRY_LAYOUT.byteSize());
      this.memory.set(
          ValueLayout.ADDRESS,
          LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement(HASH_SET_ENTRIES_NAME)),
          offHeap);
    } else {
      final MemorySegment ms =
          this.memory.get(
              ValueLayout.ADDRESS,
              LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement(HASH_SET_ENTRIES_NAME)));
      offHeap = ms.reinterpret(this.capacity * ENTRY_LAYOUT.byteSize());
    }

    // Bulk copy of entries to off-heap memory segment.
    offHeap.copyFrom(MemorySegment.ofBuffer(this.entries.rewind()));

    // Then set the capacity and size.
    this.memory.set(
        ValueLayout.JAVA_INT,
        LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement(HASH_SET_CAPACITY_NAME)),
        this.capacity);
    this.memory.set(
        ValueLayout.JAVA_INT,
        LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement(HASH_SET_SIZE_NAME)),
        this.size);
  }

  /**
   * Inserts the given {@code key} and {@code value} into this hash set. If a new entry must be
   * inserted (instead of one being overwritten) and the fill factor exceeds {@link
   * OffHeapHashSet#MAX_LOAD_FACTOR}, the table will be grown and rehashed first.
   *
   * @param key The entry key to insert.
   * @param value The entry value.
   * @throws ArithmeticException If inserting the key/value causes the on-heap memory size to
   *     overflow {@link Integer#MAX_VALUE}.
   */
  public void put(final int key, final long value) {
    // Calculating the bucket must be done twice if there is no current entry to be replaced and the
    // hash set must be grown.
    // This might feel inefficient, but greedy allocation is more expensive than walking some
    // entries twice.
    int bucket = this.bucketOf(key, value);
    if (nonNull(this.getEntryAt(bucket))) {
      // If the entry is non-null, it contains the same key and must be overwritten.
      this.setEntryAt(bucket, new Entry(key, value));
    } else {

      // Now we know that we must insert a new Entry. Check the fill factor to see if we must grow
      // (and rehash) first.
      if (this.loadFactor() >= MAX_LOAD_FACTOR) {
        this.grow();
      }

      this.setEntryAt(this.bucketOf(key, value), new Entry(key, value));
      this.size++;
    }
  }

  /**
   * Retrieves the entry of {@code value} and returns its key.
   *
   * @param value The value to retrieve the key of.
   * @return The key, or {@code -1} if the value is not mapped in this hash set.
   */
  public int keyOf(final long value) {
    int hash = Long.hashCode(value);

    int bucket = hash % this.capacity;
    Entry entry;
    while (nonNull(entry = this.getEntryAt(bucket))) {
      if (entry.value == value) {
        return entry.key;
      }

      bucket++;
      bucket %= this.capacity;
    }

    return -1;
  }

  /**
   * Returns the amount of elements in this set.
   *
   * @return The amount of elements contained in this {@link OffHeapHashSet}.
   */
  public int size() {
    return this.size;
  }

  /**
   * Implementation of elements within a {@link OffHeapHashSet}.
   *
   * @author houthacker
   */
  private record Entry(int key, long value) {}
}
