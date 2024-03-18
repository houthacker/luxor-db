package dev.luxor.server.wal;

import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;

import dev.luxor.server.io.LuxorFile;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Iterator;
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
 * @implNote The constructor of this class throws an exception, which makes it vulnerable to
 *     finalizer attacks. To mitigate that, this class is made final. If the need to extend this
 *     class arises, it is obviously required to find another mitigation.
 * @author houthacker
 */
@SuppressWarnings("PMD.TooManyMethods")
public final class OffHeapHashSet implements WALIndexTable {

  private static final Logger log = LoggerFactory.getLogger(OffHeapHashSet.class);

  /** The name of the 'key' field in the hash_set_entry struct. */
  private static final String KEY_NAME = "key";

  /** The name of the 'value' field in the hash_set_entry struct. */
  private static final String VALUE_NAME = "value";

  /** The memory layout of an OffHeapHashSet.Entry in off-heap memory. */
  private static final MemoryLayout ENTRY_LAYOUT =
      MemoryLayout.structLayout(
              ValueLayout.JAVA_INT.withName(KEY_NAME),
              MemoryLayout.paddingLayout(ValueLayout.JAVA_INT.byteSize()),
              ValueLayout.JAVA_LONG.withName(VALUE_NAME))
          .withName("hash_set_entry");

  /** The byte offset of the 'key' field in a hash_set_entry struct. */
  private static final long KEY_OFFSET =
      ENTRY_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement(KEY_NAME));

  /** The byte offset of the 'value' field in a hash_set_entry struct. */
  private static final long VALUE_OFFSET =
      ENTRY_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement(VALUE_NAME));

  /** The name of the 'capacity' field in the hash_set struct. */
  private static final String CAPACITY_NAME = "capacity";

  /** The name of the 'size' field in the hash_set struct. */
  private static final String SIZE_NAME = "size";

  /** The name of the 'data' field in the hash_set struct. */
  private static final String DATA_NAME = "data";

  /** The memory layout of an OffHeapHashSet in off-heap memory. */
  public static final MemoryLayout LAYOUT =
      MemoryLayout.structLayout(
              ValueLayout.JAVA_INT.withName(CAPACITY_NAME),
              ValueLayout.JAVA_INT.withName(SIZE_NAME),
              ValueLayout.ADDRESS.withTargetLayout(ENTRY_LAYOUT).withName(DATA_NAME))
          .withName("hash_set");

  /** The byte offset of the 'size' field in the hash_set struct. */
  private static final long SIZE_OFFSET =
      LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement(SIZE_NAME));

  /** The byte offset of the 'capacity' field in the hash_set struct. */
  private static final long CAPACITY_OFFSET =
      LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement(CAPACITY_NAME));

  /**
   * When an {@link OffHeapHashSet} becomes fairly large (roughly 10Mb), this fact is logged to the
   * appropriate output. Because the contents of the memory-mapped off-heap area are copied into an
   * {@link OffHeapHashSet}, having a fairly large set can quickly saturate the available JVM
   * allotted memory, since each client connection (carrier thread) has its own {@link
   * OffHeapHashSet} to store the WAL index in.
   */
  @SuppressWarnings(
      "PMD.LongVariable") // variable name would be less intuitive if renamed to a shorter version.
  private static final int SIZE_WARNING_THRESHOLD = 655_360;

  /**
   * If an element is inserted while the load factor is at least this value, the hash set is grown.
   */
  private static final double MAX_LOAD_FACTOR = 0.75d;

  /** The initial capacity of this {@link OffHeapHashSet} if the off-heap capacity is 0. */
  private static final int INITIAL_CAPACITY = 4096;

  /** The backing file. */
  private final LuxorFile file;

  /** The storage of this set's metadata. */
  private final MemorySegment header;

  /** The file offset at which the header is stored. */
  private final long headerOffset;

  /** The storage of this set's data. */
  private MemorySegment data;

  /** Whether the table is currently being grown. This is used for recursive grow detection. */
  private boolean growing;

  /**
   * Creates a new {@link OffHeapHashSet} using the provided {@code file} to allocate new space when
   * required.
   *
   * @param file The memory-mapped file.
   * @param requestedOffset The minimum offset in the memory-mapped file for the {@link
   *     MemorySegment} of this HashSet.
   * @throws IOException If memory mapping the required file regions fails.
   */
  public OffHeapHashSet(final LuxorFile file, final long requestedOffset) throws IOException {
    this.file = requireNonNull(file, "file");
    this.headerOffset = align(requestedOffset, LAYOUT.byteAlignment());

    if (log.isTraceEnabled() && this.headerOffset != requestedOffset) {
      log.trace(
          "Storing OffHeapHashSet at aligned offset {} instead of requested offset {}.",
          this.headerOffset,
          requestedOffset);
    }

    this.header = this.file.mapShared(this.headerOffset, LAYOUT.byteSize());
    this.data = this.acquireData();
  }

  /**
   * Aligns {@code offset} to {@code alignment}. The returned value is never less than {@code
   * offset}.
   *
   * @param offset The offset to align.
   * @param alignment The alignment to align to.
   * @return The aligned offset.
   */
  private static long align(final long offset, final long alignment) {
    final long remainder = offset % alignment;

    return remainder == 0 ? offset : offset + remainder;
  }

  /**
   * Fills {@code memory} with empty entries, returning the same segment afterward.
   *
   * @param memory The memory to fill.
   * @return The filled memory segment.
   */
  private static MemorySegment fillWithEmptyEntries(final MemorySegment memory) {

    memory
        .elements(ENTRY_LAYOUT)
        .forEach(
            entry -> {
              entry.set(
                  ValueLayout.JAVA_LONG,
                  KEY_OFFSET,
                  0xffffffff00000000L); // int(-1) and zeroed padding
              entry.set(ValueLayout.JAVA_LONG, VALUE_OFFSET, -1L);
            });

    return memory;
  }

  /**
   * Reads or allocates and initializes the {@link MemorySegment} containing the table data.
   *
   * @return The memory containing the table data.
   * @throws IOException If allocating the required memory fails.
   */
  private MemorySegment acquireData() throws IOException {
    final int capacity = this.capacity();
    MemorySegment dataSegment;
    if (capacity > 0) {
      log.trace("Read OffHeapHashSet.data from pre-allocated memory.");

      dataSegment =
          this.header
              .get(
                  ValueLayout.ADDRESS,
                  LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement(DATA_NAME)))
              .reinterpret(capacity * ENTRY_LAYOUT.byteSize());
    } else {

      // Getting here means the off-heap memory table contains no data yet. Allocate it and let the
      // header point to it.
      log.trace(
          "Allocate memory for OffHeapHashSet.data and read from that, since we're still empty.");
      dataSegment =
          fillWithEmptyEntries(
              this.file.mapShared(
                  this.headerOffset + LAYOUT.byteSize(),
                  INITIAL_CAPACITY * ENTRY_LAYOUT.byteSize()));
      this.header.set(ValueLayout.JAVA_INT, SIZE_OFFSET, 0);
      this.header.set(ValueLayout.JAVA_INT, CAPACITY_OFFSET, INITIAL_CAPACITY);
      this.header.set(
          ValueLayout.ADDRESS,
          LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement(DATA_NAME)),
          dataSegment);
    }
    return dataSegment;
  }

  /**
   * Returns the entry at the given {@code index}.
   *
   * @param memory The memory segment to read the entry from.
   * @param index The index at which the entry resides.
   * @return The entry, or {@code null} if no entry resides at the given index.
   */
  @SuppressWarnings("PMD.OnlyOneReturn") // require logging in different exit points
  private Entry getEntryAt(final MemorySegment memory, final int index) {
    final long entryOffset = Math.multiplyExact(index, ENTRY_LAYOUT.byteSize());

    final int key = memory.get(ValueLayout.JAVA_INT, entryOffset);
    final long value = memory.get(ValueLayout.JAVA_LONG, entryOffset + VALUE_OFFSET);

    if (key == -1) {
      log.trace("OffHeapHashSet.getEntryAt({}) (offset {}) is empty.", index, entryOffset);
      return null;
    }

    log.trace("OffHeapHashSet.getEntryAt({}) (offset {}) is empty.", index, entryOffset);
    return new Entry(key, value);
  }

  /**
   * Sets the entry at the given {@code index}.
   *
   * @param memory The memory segment to store the entry in.
   * @param index The index at which to set the entry.
   * @param entry The entry to set.
   */
  private void setEntryAt(final MemorySegment memory, final int index, final Entry entry) {
    final long entryOffset = Math.multiplyExact(index, ENTRY_LAYOUT.byteSize());

    memory.set(ValueLayout.JAVA_INT, entryOffset, entry.key);
    memory.set(ValueLayout.JAVA_LONG, entryOffset + VALUE_OFFSET, entry.value);
  }

  /**
   * Calculates the current load factor.
   *
   * @return The current load factor.
   */
  private double loadFactor() {
    return this.size() / (this.capacity() * 1.0d);
  }

  /**
   * Grows this {@link OffHeapHashSet} to double its size. After allocating the new memory, the data
   * are re-hashed.
   *
   * @throws ArithmeticException if growing the capacity would cause an integer overflow.
   * @throws IOException If either mapping a larger region fails or a recursive {@code grow()} is
   *     detected.
   * @throws OutOfMemoryError If less JVM heap memory is available than is required to grow this
   *     table.
   * @throws RecursiveGrowException If this method is detected to be called recursively.
   */
  private MemorySegment grow() throws IOException {
    if (this.growing) {
      throw new RecursiveGrowException(
          "OffHeapHashSet.grow() is called recursively. Reverting to pre-grow state.");
    }

    this.growing = true; // NOPMD (UnusedAssignment false positive)
    try {
      final int oldSize = this.size();
      final int oldCapacity = this.capacity();
      final int newCapacity = oldCapacity << 1;
      if (newCapacity < 0) {
        throw new ArithmeticException("Integer overflow.");
      }
      log.debug(
          "Growing OffHeapHashSet capacity from {} to {} (size = {}).",
          oldCapacity,
          newCapacity,
          oldSize);

      // Copy the old entries memory to a temporary off-heap location. Ensure it is automatically
      // cleaned up after use.
      try (Arena arena = Arena.ofConfined()) {
        final MemorySegment temp =
            arena.allocate(ENTRY_LAYOUT.byteSize() * oldCapacity).copyFrom(this.data);

        final MemorySegment extendedData =
            fillWithEmptyEntries(
                this.file.mapShared(
                    this.headerOffset + LAYOUT.byteSize(), ENTRY_LAYOUT.byteSize() * newCapacity));

        // Size must be set to 0 to put the pre-existing entries in their correct bucket.
        this.size(0);
        this.capacity(newCapacity);

        // Now that we have the new memory segment allocated, fill it with the entries from the old
        // segment.
        this.rehashEntries(oldSize, oldCapacity, temp, extendedData);

        // Getting here means we were successful in copying all entries from old to new memory.
        this.header.set(
            ValueLayout.ADDRESS,
            LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement(DATA_NAME)),
            extendedData);
        this.data = extendedData;
        return this.data;
      }
    } finally {
      this.growing = false;
    }
  }

  /**
   * Copies the entries from {@code source} to {@code destination}, rehashing them to their newly
   * allocated buckets.
   *
   * @param sourceSize The amount of entries in the source segment.
   * @param sourceCapacity The maximum amount of entries in the source segment.
   * @param source The segment to copy the entries from.
   * @param destination The segment to cop the entries to.
   * @throws IOException If {@code destination} must be grown in order to fit the entries from
   *     {@code source}.
   */
  private void rehashEntries(
      final int sourceSize,
      final int sourceCapacity,
      final MemorySegment source,
      final MemorySegment destination)
      throws IOException {
    for (final Iterator<MemorySegment> it = source.elements(ENTRY_LAYOUT).iterator();
        it.hasNext(); ) {
      final MemorySegment slice = it.next();
      final int key = slice.get(ValueLayout.JAVA_INT, KEY_OFFSET);
      final long value = slice.get(ValueLayout.JAVA_LONG, VALUE_OFFSET);

      if (key != -1) {
        try {
          this.putInternal(destination, key, value);
        } catch (IOException e) {
          this.size(sourceSize);
          this.capacity(sourceCapacity);

          // this.data and ptr to it do not have to be set since they have not been updated yet.
          // We will have to bail out now though, so rethrowing the original exception.
          final String message =
              "It appears that the OffHeapHashSet is grown recursively and that went undetected."
                  + "Reverting to pre-grow state.";
          throw new IOException(message, e);
        } catch (RecursiveGrowException e) {
          this.size(sourceSize);
          this.capacity(sourceCapacity);

          throw e;
        }
      }
    }
  }

  /**
   * Calculates the bucket for the given {@code key} and {@code value} must be stored in. Although
   * this method walks all entries until it found an empty slot, this is guaranteed to terminate if
   * {@link OffHeapHashSet#MAX_LOAD_FACTOR} is {@code < 1.0}.
   *
   * @param memory The memory segment to search.
   * @param key The entry key.
   * @param value The entry value.
   * @return The bucket to store the entry in.
   */
  private int bucketOf(final MemorySegment memory, final int key, final long value) {
    final int hash = Long.hashCode(value);

    final int capacity = this.capacity();
    int bucket = hash % capacity;
    Entry entry;
    while (nonNull(entry = this.getEntryAt(memory, bucket))) {
      if (entry.key == key) {
        break;
      }

      bucket++;
      bucket %= capacity;
    }

    return bucket;
  }

  /**
   * Used to add client data as well as growing the table data, this method stores the entry of
   * {@code key} and {@code value} into the appropriate bucket in {@code memory}. Any pre-existing
   * entries with the same key are overwritten.
   *
   * @param memory The memory to store the entry in.
   * @param key The entry key.
   * @param value The entry value.
   * @throws IOException If the table must be grown and that fails.
   */
  private void putInternal(final MemorySegment memory, final int key, final long value)
      throws IOException {
    // Calculating the bucket must be done twice if there is no current entry to be replaced and the
    // hash set must be grown.
    // This might feel inefficient, but greedy allocation is more expensive than walking some
    // entries twice.
    final int bucket = this.bucketOf(memory, key, value);
    if (nonNull(this.getEntryAt(memory, bucket))) {
      log.trace("Overwriting entry ({}, {}) at bucket {}.", key, value, bucket);

      // If the entry is non-null, it contains the same key and must be overwritten.
      this.setEntryAt(memory, bucket, new Entry(key, value));
    } else {

      // Now we know that we must insert a new Entry. Check the fill factor to see if we must grow
      // (and rehash) first.
      final MemorySegment segmentToUse =
          this.loadFactor() >= MAX_LOAD_FACTOR ? this.grow() : memory;

      log.trace("Add new entry ({}, {}) at bucket {}.", key, value, bucket);

      this.setEntryAt(segmentToUse, this.bucketOf(segmentToUse, key, value), new Entry(key, value));
      final int newSize = this.size() + 1;
      this.size(newSize);

      if (newSize >= SIZE_WARNING_THRESHOLD) {
        log.warn("OffHeapHashSet with size {} might cause JVM heap saturation.", newSize);
      }
    }
  }

  /**
   * Stores {@code size} in the table header data.
   *
   * @param size The amount of entries in the table.
   */
  private void size(final int size) {
    this.header.set(ValueLayout.JAVA_INT, SIZE_OFFSET, size);
  }

  /**
   * Returns the maximum amount of entries that can be stored in this table without growing it.
   *
   * @return The table capacity.
   */
  private int capacity() {
    return this.header.get(ValueLayout.JAVA_INT, CAPACITY_OFFSET);
  }

  /**
   * Stores {@code capacity} in the table header data.
   *
   * @param capacity The maximum amount of entries that can be stored in this table without growing
   *     it.
   */
  private void capacity(final int capacity) {
    this.header.set(ValueLayout.JAVA_INT, CAPACITY_OFFSET, capacity);
  }

  /** Reloads the header segment of this {@link OffHeapHashSet} from off-heap memory. */
  public void reload() {
    this.header.load();
    final MemorySegment segment =
        this.header.get(
            ValueLayout.ADDRESS,
            LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement(DATA_NAME)));
    this.data = segment.reinterpret(this.capacity() * ENTRY_LAYOUT.byteSize());
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
   * @throws UncheckedIOException If growing the set is required but fails.
   */
  @Override
  public void put(final int key, final long value) {
    try {
      this.putInternal(this.data, key, value);
    } catch (IOException e) {
      throw new UncheckedIOException("Cannot grow hash set.", e);
    }
  }

  /**
   * Searches the entry of {@code value} and returns its key.
   *
   * @param value The value to retrieve the key of.
   * @return The key, or {@code -1} if the value is not mapped in this hash set.
   */
  @Override
  public int keyOf(final long value) {
    final int hash = Long.hashCode(value);

    final int capacity = this.capacity();
    int bucket = hash % capacity;
    int keyOfValue = -1;
    Entry entry;
    while (nonNull(entry = this.getEntryAt(this.data, bucket))) {
      if (entry.value == value) {
        keyOfValue = entry.key;
        break;
      }

      bucket++;
      bucket %= capacity;
    }

    return keyOfValue;
  }

  /**
   * Returns the amount of entries contained in this set.
   *
   * @return The amount of entries.
   */
  @Override
  @SuppressWarnings(
      "checkstyle:OverloadMethodsDeclarationOrder") // false positive with private void size(int)
  public int size() {
    return this.header.get(ValueLayout.JAVA_INT, SIZE_OFFSET);
  }

  /**
   * Implementation of elements within a {@link OffHeapHashSet}.
   *
   * @author houthacker
   */
  private record Entry(int key, long value) {}
}
