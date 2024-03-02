package dev.luxor.server.io;

import dev.luxor.server.concurrent.Locks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

/**
 * A {@link FileSerial} is a unique identification of a given file or directory within its filesystem, and should
 * be used to synchronize access between threads to ths underlying resource.
 *
 * @author houthacker
 */
public class FileSerial {

    private static final Logger log = LoggerFactory.getLogger(FileSerial.class);

    /**
     * The linked list of all FileSerials. The list is always searched backwards, because the last FileSerial often
     * has the highest change of reuse.
     */
    private static FileSerial tail = null;
    private static final ReentrantReadWriteLock globalSerialLock = new ReentrantReadWriteLock();


    /**
     * The unique identifying object of this {@code FileSerial}
     */
    private final Object key;

    /**
     * The r/w lock to (un)lock this {@code FileSerial}.
     */
    private final ReentrantReadWriteLock mutex;

    /**
     * The amount of objects (in the current JVM) that refer to this {@code FileSerial}.
     */
    private final AtomicInteger refCount;

    /**
     * The next {@code FileSerial}, if any.
     */
    private FileSerial next;

    /**
     * The previous {@code FileSerial}, if any.
     */
    private FileSerial previous;

    private FileSerial(final Object key) {
        this.key = key;
        this.mutex = new ReentrantReadWriteLock();
        this.refCount = new AtomicInteger(1);
        this.next = null;
        this.previous = null;
    }

    /**
     * Acquires a unique identification of the given path. Multiple paths leading to the same location will yield the
     * same {@link FileSerial}.
     *
     * @implNote All {@link FileSerial} instances are stored in a linked list to enable finding them later, ensuring
     * the same instance is used for paths resolving to the same unique serial.<br />
     * Access to this linked list is synchronized, so if any structural changes must be made to this linked list
     * (for example inserting a new {@link FileSerial} or if another serial is being removed by another thread), this
     * method blocks on those operations before returning the {@link FileSerial}.
     *
     * @param path The path to identify.
     * @return The serial of the path.
     * @throws IOException If an I/O error occurs while reading the file attributes, or if the path cannot be resolved
     * to an absolute path.
     */
    public static FileSerial find(final Path path) throws IOException {
        final BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes.class);

        Object key = attrs.fileKey();
        if (isNull(key)) {
            log.debug("The filesystem containing file at {} provides no unique file serial; using real path name as fallback", path);
            key = path.toRealPath().toString();
        }

        FileSerial serial = tail;

        // Prevent structural changes in the linked list while traversing it.
        globalSerialLock.readLock().lock();
        try {
            while (nonNull(serial)) {
                if (serial.key == key || serial.key.equals(key)) {
                    serial.refCount.incrementAndGet();

                    return serial;
                }

                serial = serial.previous;
            }
        } finally {
            globalSerialLock.readLock().unlock();
        }

        log.trace("FileSerial of path {} not yet referenced; creating new reference.", path);
        serial = new FileSerial(key);
        serial.next = tail;

        if (isNull(tail)) {

            // If the list is empty, serial is by definition the last element.
            tail = serial;
        } else {

            // Otherwise, insert serial between tail and tail.previous
            serial.previous = tail.previous;

            // Acquire the global write lock to execute structural changes in the linked list.
            globalSerialLock.writeLock().lock();
            try {
                final Lock tailLock = tail.mutex.writeLock();
                tailLock.lock();
                try {
                    tail.previous = serial;
                } finally {
                    tailLock.unlock();
                }

                if (nonNull(serial.previous)) {
                    final Lock previousLock = serial.previous.mutex.writeLock();
                    previousLock.lock();
                    try {
                        serial.previous.next = serial;
                    } finally {
                        previousLock.unlock();
                    }
                }
            } finally {
                globalSerialLock.writeLock().unlock();
            }
        }

        return serial;
    }

    /**
     * Decrements the reference count of this {@link FileSerial} and removes it from the global linked list if it has
     * no more references. The latter is a blocking operation for calling threads.<br />
     * This method should typically be used
     */
    public void dereference() {
        // If decreasing the reference count yields zero, remove the FileSerial from the linked list.
        if (this.referenceCount() > 0 && this.refCount.decrementAndGet() == 0) {
            log.trace("FileSerial has no more references; removing it from the linked list.");

            // Removing the serial is a structural change, acquire the write lock.
            globalSerialLock.writeLock().lock();
            try {
                if (nonNull(this.previous) && nonNull(this.next)) {

                    Locks.exclusiveLock(this.previous.mutex, this.next.mutex);
                    try {
                        this.previous.next = this.next;
                        this.next.previous = this.previous;
                    } finally {
                        Locks.exclusiveUnlock(this.previous.mutex, this.next.mutex);
                    }
                } else if (nonNull(this.previous)) {
                    final Lock writeLock = this.previous.mutex.writeLock();
                    writeLock.lock();
                    try {
                        this.previous.next = this.next;
                    } finally {
                        writeLock.unlock();
                    }
                } else if (nonNull(this.next)) {
                    final Lock writeLock = this.next.mutex.writeLock();
                    writeLock.lock();
                    try {
                        this.next.previous = this.previous;
                    } finally {
                        writeLock.unlock();
                    }
                }

                // If this is the last inode, reset the tail pointer.
                if (tail == this) {
                    tail = null;
                }

            } finally {
                globalSerialLock.writeLock().unlock();
            }
        }
    }

    /**
     * @return The mutex to use when operations relating to this {@link FileSerial} must be synchronized with other
     * threads.
     */
    public ReentrantReadWriteLock mutex() {
        return this.mutex;
    }

    /**
     * @return The amount of files referencing this {@link FileSerial} in the current JVM instance.
     */
    int referenceCount() {
        return this.refCount.get();
    }

    @Override
    public String toString() {
        return String.format("FileSerial(refs=%d,serial=[%s])", this.referenceCount(), this.key);
    }
}
