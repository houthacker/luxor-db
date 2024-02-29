package dev.luxor.server.algo;

import java.nio.ByteBuffer;

import static java.util.Objects.nonNull;

/**
 * FNV-1a is a fast hashing algorithm for use in a non-cryptographic context.
 *
 * @author houthacker
 */
public class FNV1a {

    private static final long OFFSET_BASIS = 0xcbf29ce484222325L;

    private static final long FNV_PRIME = 0x100000001b3L;

    private long state;

    public FNV1a() {
        this.state = OFFSET_BASIS;
    }

    /**
     * Calculates the FNV1a hash of the given message, using its values at index {@code start} to {@code length}.
     * If the given message is {@code null}, this method will have no effect.
     *
     * @param message The message to hash.
     * @param start The start index (inclusive).
     * @param length The amount of bytes to hash.
     * @return This instance.
     */
    public FNV1a iterate(final ByteBuffer message, int start, int length) {
        if (nonNull(message)) {
            for (int i = start; i < start + length; i++) {
                this.state *= FNV_PRIME;
                this.state ^= message.get(i);
            }
        }

        return this;
    }

    /**
     * @return The current state.
     */
    public long state() {
        return this.state;
    }
}
