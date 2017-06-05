package com.bow.camptotheca.mr;

/**
 * Implementation of a key-value pair to be used in the map-reduce algorithm.
 * For simplicity, both keys and values are taken as Strings.
 * 
 * @author Sylvain Hallé
 * @version 1.1
 *
 */
public class Tuple<K, V> {

    private K m_key = null;

    private V m_value = null;

    /**
     * Create an empty tuple
     */
    public Tuple() {
        super();
    }

    /**
     * Create a tuple with given key and value
     * 
     * @param key The key
     * @param value The value
     */
    public Tuple(K key, V value) {
        this();
        setKey(key);
        setValue(value);
    }

    /**
     * Set the key for the tuple
     * 
     * @param key Value of the key
     */
    public void setKey(K key) {
        if (key == null)
            m_key = null;
        else
            m_key = key;
    }

    /**
     * Get the tuple's key
     * 
     * @return The tuple's key
     */
    public K getKey() {
        return m_key;
    }

    /**
     * Get the tuple's value
     * 
     * @return The tuple's value
     */
    public V getValue() {
        return m_value;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null)
            return false;
        assert o != null;
        if (o instanceof Tuple<?, ?>) {
            return equals((Tuple<?, ?>) o);
        }
        return false;
    }

    public boolean equals(Tuple<K, V> t) {
        assert t != null;
        return m_key.equals(t.m_key) && m_value.equals(t.m_value);
    }

    @Override
    public int hashCode() {
        return m_key.hashCode() + m_value.hashCode();
    }

    /**
     * Set the tuple's value
     * 
     * @param value The value's value (!)
     */
    public void setValue(V value) {
        if (value == null)
            m_value = null;
        else
            m_value = value;
    }

    @Override
    public String toString() {
        return "\u2329" + m_key + "," + m_value + "\u232A";
    }
}
