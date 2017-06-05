package com.bow.camptotheca.mr;

import java.util.Iterator;

/**
 * Data source used as the input of the map and reduce phases. An InCollector
 * can be used to enumerate data tuples using the {@link Collector#hasNext()}
 * and {@link Collector#next()} methods, like an {@link Iterator}.
 * 
 * @author Sylvain Hallé
 * @version 1.0
 *
 */
public interface InCollector<K, V> extends Iterator<Tuple<K, V>> {
    /**
     * Count the number of tuples in the collector
     * 
     * @return The number of tuples. A Collector for which the size cannot be
     *         computed should return -1.
     */
    int count();

    /**
     * Rewinds the collector to the beginning of its enumeration
     */
    void rewind();
}
