
package com.bow.camptotheca.mr;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Data source used both as the input and output of the map and reduce phases. A
 * Collector can be used to:
 * <ol>
 * <li>Store data tuples using the {@link Collector#collect(Tuple)} method</li>
 * <li>Enumerate data tuples using the {@link Collector#hasNext()} and
 * {@link Collector#next()} methods, like an {@link Iterator}</li>
 * <li>Partition the set of tuples into a set of Collectors, grouping tuples by
 * their key, using the {@link Collector#subCollector(Object)} and
 * {@link Collector#subCollectors()} methods</li>
 * </ol>
 * 
 * @author Sylvain Hallé
 * @version 1.1
 *
 */
public class Collector<K, V> implements InCollector<K, V>, OutCollector<K, V> {
    private List<Tuple<K, V>> tuples = new LinkedList<Tuple<K, V>>();

    private Iterator<Tuple<K, V>> iterator = null;

    /**
     * Return the Collector's contents as a list of tuples
     * 
     * @return The list of tuples
     */
    public List<Tuple<K, V>> toList() {
        return tuples;
    }

    /**
     * Add a collection of tuples to the Collector
     * 
     * @param list A collection of {@link Tuple}
     */
    public void addAll(Collection<Tuple<K, V>> list) {
        synchronized (this) {
            tuples.addAll(list);
        }
    }

    /**
     * Add a new tuple to the Collector in a synchronized mode
     * 
     * @param t The {@link Tuple} to add
     */
    public void collect(Tuple<K, V> t) {
        synchronized (this) {
            tuples.add(t);
        }
    }

    /**
     * Returns a new Collector whose content is made of all tuples with given
     * key
     * 
     * @param key The key to find
     * @return A new {@link Collector}
     */
    public Collector<K, V> subCollector(K key) {
        Collector<K, V> c = new Collector<K, V>();
        synchronized (this) {
            for (Tuple<K, V> t : tuples) {
                if (t.getKey().equals(key))
                    c.tuples.add(t);
            }
        }
        return c;

    }

    public int count() {
        synchronized (this) {
            return tuples.size();
        }
    }

    /**
     * Partitions the set of tuples into new collectors, each containing all
     * tuples with the same key
     * 
     * @return A map from keys to Collectors
     */
    public Map<K, Collector<K, V>> subCollectors() {
        Map<K, Collector<K, V>> out = new HashMap<K, Collector<K, V>>();

        synchronized (this) {
            for (Tuple<K, V> t : tuples) {
                K key = t.getKey();
                Collector<K, V> c = out.get(key);

                if (c == null) {
                    c = new Collector<K, V>();
                }

                c.collect(t);
                out.put(key, c);
            }
        }
        return out;
    }

    @Override
    public boolean hasNext() {
        if (iterator == null)
            iterator = tuples.iterator();
        return iterator.hasNext();
    }

    @Override
    public Tuple<K, V> next() {
        return iterator.next();
    }

    @Override
    public void remove() {
        iterator.remove();
    }

    @Override
    public String toString() {
        return tuples.toString();
    }

    @Override
    public void rewind() {
        iterator = null;
    }
}
