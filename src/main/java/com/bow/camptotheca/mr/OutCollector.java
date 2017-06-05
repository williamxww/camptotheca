package com.bow.camptotheca.mr;

/**
 * Data source used as the output of the map and reduce phases. An OutCollector
 * can be used to store data tuples using the
 * {@link OutCollector#collect(Tuple)} method.
 *
 * @author Sylvain Hallé
 * @version 1.1
 *
 */
public interface OutCollector<K, V> {

    /**
     * Add a new tuple to the Collector
     * 
     * @param t The {@link Tuple} to add
     */
    void collect(Tuple<K, V> t);

    /**
     * Rewinds the collector to the beginning of its enumeration
     */
    void rewind();
}
