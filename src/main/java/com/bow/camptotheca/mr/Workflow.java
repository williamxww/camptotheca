package com.bow.camptotheca.mr;

/**
 * Coordinates the execution of a map-reduce job.
 * 
 * @author Sylvain Hallé
 * @version 1.1
 *
 */
public interface Workflow<K, V> {
    /**
     * Start a map-reduce job and output the results as a single Collector
     * containing all output tuples.
     * 
     * @return An InputCollector containing all output tuples
     */
    InCollector<K, V> run();
}
