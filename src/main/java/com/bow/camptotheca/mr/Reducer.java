package com.bow.camptotheca.mr;

/**
 * Interface declaration of the reduce phase of the map-reduce algorithm.
 * 
 * @author Sylvain Hallé
 * @version 1.1
 *
 */
public interface Reducer<K, V> {
    /**
     * Reduce function
     * 
     * @param out A {@link OutCollector} that will be used to write output
     *        tuples
     * @param key The key associated to this instance of reducer
     * @param in An {@link InCollector} containing all the tuples generated in
     *        the map phase for the given key. map阶段收集的tuple
     */
    void reduce(OutCollector<K, V> out, K key, InCollector<K, V> in);
}
