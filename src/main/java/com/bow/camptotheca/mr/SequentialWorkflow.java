package com.bow.camptotheca.mr;

import java.util.Map;
import java.util.Set;

/**
 * Coordinates the execution of a map-reduce job in a single thread. This means
 * that the data source is fed tuple by tuple to the mapper, the output tuples
 * are collected, split according to their keys, and each list is sent to the
 * reducer, again in a sequential fashion. As such, the SequentialWorkflow
 * reproduces exactly the processing done by map-reduce, without the
 * distribution of computation. It is best suited to pedagogical and debugging
 * purposes.
 * 
 * @author Sylvain Hallé
 * @version 1.1
 *
 */
public class SequentialWorkflow<K, V> implements Workflow<K, V> {
    private Mapper<K, V> mapper = null;

    private Reducer<K, V> reducer = null;

    private InCollector<K, V> source = null;

    /**
     * The total number of tuples that the mappers will produce. This is only
     * necessary for gathering statistics, and is not required in the MapReduce
     * processing <i>per se</i>.
     */
    protected long totalTuples = 0;

    /**
     * The maximum number of tuples that a single reducer will process. This is
     * used as a measure of the "linearity" of the MapReduce job: assuming all
     * reducers worked on a separate thread, this value is proportional to the
     * time the longest reducer would take. Intuitively, the ratio
     * maxTuples/totalTuples indicates the "speedup" incurred by the use of
     * parallel reducers compared to a strictly linear processing.
     */
    protected long maxTuples = 0;

    /**
     * Create an instance of SequentialWorkflow.
     * 
     * @param m The {@link Mapper} to use in the map phase
     * @param r The {@link Reducer} to use in the reduce phase
     * @param c The {@link InCollector} to use as the input source of tuples
     */
    public SequentialWorkflow(Mapper<K, V> m, Reducer<K, V> r, InCollector<K, V> c) {
        super();
        setMapper(m);
        setReducer(r);
        setSource(c);
    }

    public void setMapper(Mapper<K, V> m) {
        mapper = m;
    }

    public void setReducer(Reducer<K, V> r) {
        reducer = r;
    }

    public void setSource(InCollector<K, V> c) {
        source = c;
    }

    public InCollector<K, V> run() {
        if (mapper == null || reducer == null || source == null) {
            return null;
        }
        assert mapper != null;
        assert reducer != null;
        assert source != null;
        Collector<K, V> tempCollector = new Collector<K, V>();
        source.rewind();
        while (source.hasNext()) {
            Tuple<K, V> t = source.next();
            //对原始数据进行处理获取想要的数据格式
            mapper.map(tempCollector, t);
        }

        // 分组，key相同的tuple放到一个集合里面
        Map<K, Collector<K, V>> shuffle = tempCollector.subCollectors();
        Set<K> keys = shuffle.keySet();
        Collector<K, V> out = new Collector<K, V>();
        for (K key : keys) {
            Collector<K, V> sameKeyCollector = shuffle.get(key);
            int tupleNum = sameKeyCollector.count();
            totalTuples += tupleNum;
            maxTuples = Math.max(maxTuples, tupleNum);
            //对一组key相同的tuple进行合并
            reducer.reduce(out, key, sameKeyCollector);
        }
        return out;
    }

    /**
     * Returns the maximum number of tuples processed by a single reducer in the
     * process. This method returns 0 if the MapReduce job hasn't executed yet
     * (i.e. you should call it only after a call to
     * {@link SequentialWorkflow#run()}).
     * 
     * @return The number of tuples
     */
    public long getMaxTuples() {
        return maxTuples;
    }

    /**
     * Returns the total number of tuples processed by all reducers. This method
     * returns 0 if the MapReduce job hasn't executed yet (i.e. you should call
     * it only after a call to {@link SequentialWorkflow#run()}).
     * 
     * @return The number of tuples
     */
    public long getTotalTuples() {
        return totalTuples;
    }

}
