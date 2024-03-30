package dev.luxor.server.wal;

import java.util.Spliterator;

/**
 * a {@link WALSpliterator} traverses over all {@link WALFrame}s in a {@link WriteAheadLog}. Whether
 * the frames are valid or current is of no concern to this spliterator.
 *
 * @author houthacker
 */
public interface WALSpliterator extends Spliterator<WALFrame> {}
