package com.bow.camptotheca.mr;

import java.util.LinkedList;
import java.util.List;

/**
 * Coordinates the creation of all threads needed to execute the jobs. This
 * means that it's the only object able to create mapper Threads and reducer
 * Threads with all of the informations needed. It's also very important to know
 * there is a management on the number of threads. The manager create a thread,
 * put it in the threads list and send the object to the calling line. If the
 * maximum of threads has been hit, he don't stop to try to find a dead thread
 * in the threads list. After that, he erases the dead thread, add the new
 * thread and return the object to the calling ligne. Finally, he can check if
 * all threads of the list are dead. The goal is to make sure of all of the
 * handling is over, before to pass to the other phase.
 * 
 * @author Maxime Soucy-Boivin
 * @version 1.1
 *
 */
public class ResourceManager<K, V> {

    /**
     * The number of threads by default of the manager. This value is use when
     * the builder is call without a integer parameter.
     */
    private int threadDefault = 100;

    /**
     * The maximum number of threads that can create the manager. The value is
     * set by the builder.
     */
    private int threadMax = 0;

    /**
     * The list that contains all of the created threads of the manager. It's
     * important to know that the manager is the only one to have access to the
     * list.
     */
    private List<Thread> listThread = new LinkedList<Thread>();

    /**
     * Set the maximum of threads of the manager
     * 
     * @param max Value of the maximum
     */
    private void setThreadMax(int max) {
        this.threadMax = max;
    }

    /**
     * Returns the maximum number of threads of the manager can create
     * 
     * @return The number of threads
     */
    public int getThreadMax() {
        return this.threadMax;
    }

    /**
     * Create an instance of ResourceManager with the default value of threads
     * maximum
     */
    public ResourceManager() {
        setThreadMax(threadDefault);
    }

    /**
     * Create an instance of ResourceManager
     * 
     * @param maxThread Value of the threads maximum given by the user
     */
    public ResourceManager(int maxThread) {
        setThreadMax(maxThread);
    }

    /**
     * Creates a mapper thread
     * 
     * @param t The tuple to analyze
     * @param temp_coll The collector of all results
     * @param m_mapper The {@link Mapper} to use in the map phase
     * @return A thread
     */
    public Thread getThread(Tuple<K, V> t, Collector<K, V> temp_coll, Mapper<K, V> m_mapper) {
        int i = 0;
        boolean create = false;
        MapThread<K, V> MThread = null;
        Thread ThreadTemp = null;

        if (listThread.size() < threadMax) {
            MThread = new MapThread<K, V>(t, temp_coll, m_mapper);
            listThread.add(MThread);
        } else {
            while (create != true) {
                while (i < listThread.size()) {
                    ThreadTemp = listThread.get(i);

                    if (!ThreadTemp.isAlive()) {
                        listThread.remove(i);
                        MThread = new MapThread<K, V>(t, temp_coll, m_mapper);
                        listThread.add(MThread);
                        create = true;
                        i = listThread.size();
                    } else {
                        i++;
                    }
                }
                i = 0;
            }
        }
        return MThread;
    }

    /**
     * Creates a reducer thread
     * 
     * @param out The collector of the final results
     * @param key The key to reduce
     * @param s_source The collector of all results of the mapper phase
     * @param m_reducer The {@link Reducer} to use in the reduce phase
     * @return A thread
     */
    public Thread getThread(Collector<K, V> out, K key, Collector<K, V> s_source, Reducer<K, V> m_reducer) {
        int i = 0;
        boolean create = false;
        ReduceThread<K, V> RThread = null;
        Thread ThreadTemp = null;

        if (listThread.size() < threadMax) {
            RThread = new ReduceThread<K, V>(out, key, s_source, m_reducer);
            listThread.add(RThread);
        } else {
            while (create != true) {
                while (i < listThread.size()) {
                    ThreadTemp = listThread.get(i);

                    if (!ThreadTemp.isAlive()) {
                        listThread.remove(i);
                        RThread = new ReduceThread<K, V>(out, key, s_source, m_reducer);
                        listThread.add(RThread);
                        create = true;
                        i = listThread.size();
                    } else
                        i++;
                }
                i = 0;
            }
        }
        return RThread;
    }

    /**
     * Function who check if all threads of the list are dead and clear the list
     */
    public void waitThreads() {
        int i = 0;
        Thread ThreadTemp = null;
        while (i < listThread.size()) {
            ThreadTemp = listThread.get(i);

            if (ThreadTemp.isAlive())
                i = 0;
            else
                i++;
        }
        listThread.clear();
    }
}

/**
 * Class who encapsulates the processing of a mapper and his informations in a
 * thread
 * 
 * @author Maxime Soucy-Boivin
 */
class MapThread<K, V> extends Thread {
    /**
     * Informations needed to be transferred to the mapper For more information,
     * see function getThread
     */
    Tuple<K, V> tThread = new Tuple<K, V>();

    Collector<K, V> Thread_Temp_col = new Collector<K, V>();

    Mapper<K, V> Thread_m_mapper = null;

    /**
     * Create an instance of MapThread
     * 
     * @param t The tuple to analyse
     * @param temp_coll The collector of all results
     * @param m_mapper The {@link Mapper} to use in the map phase
     */
    MapThread(Tuple<K, V> t, Collector<K, V> temp_coll, Mapper<K, V> m_mapper) {
        this.tThread = t;
        this.Thread_Temp_col = temp_coll;
        this.Thread_m_mapper = m_mapper;
    }

    /**
     * Function who start the execution of the mapper
     */
    public void run() {
        Thread_m_mapper.map(Thread_Temp_col, tThread);
    }
}

/**
 * Class who encapsulates the processing of a reducer and his informations in a
 * thread
 * 
 * @author Maxime Soucy-Boivin
 */
class ReduceThread<K, V> extends Thread {
    /**
     * Informations needed to be transferred to the reducer For more
     * information, see function getThread
     */
    Collector<K, V> outThread = new Collector<K, V>();

    K Thread_key = null;

    Collector<K, V> Thread_s_source = new Collector<K, V>();

    Reducer<K, V> Thread_m_reducer = null;

    /**
     * Create an instance of ReduceThread
     * 
     * @param out The collector of the final results
     * @param key The key to reduce
     * @param s_source The collector of all results of the mapper phase
     * @param m_reducer The {@link Reducer} to use in the reduce phase
     */
    ReduceThread(Collector<K, V> out, K key, Collector<K, V> s_source, Reducer<K, V> m_reducer) {
        this.outThread = out;
        this.Thread_key = key;
        this.Thread_s_source = s_source;
        this.Thread_m_reducer = m_reducer;
    }

    /**
     * Function who start the execution of the reducer
     */
    public void run() {
        Thread_m_reducer.reduce(outThread, Thread_key, Thread_s_source);
    }
}