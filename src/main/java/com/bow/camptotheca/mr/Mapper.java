package com.bow.camptotheca.mr;

/**
 * Interface declaration of the map phase of the map-reduce algorithm.
 * 
 * @author Sylvain Hallé
 * @version 1.1
 *
 */
public interface Mapper<K, V> {
    /**
     * Map function , 将原始的tuple在map中处理后用collector收集起来
     * 
     * @param c A {@link OutCollector} that will be used to write output tuples
     * @param t A {@link Tuple} to process
     */
    void map(OutCollector<K, V> c, Tuple<K, V> t);
}
