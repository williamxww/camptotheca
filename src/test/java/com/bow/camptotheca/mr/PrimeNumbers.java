package com.bow.camptotheca.mr;

/**
 * Demonstration of MapReduce processing with MrSim. This program computes the
 * first 1,000 prime numbers using a map-reduce algorithm.
 *
 * 除了1和自身外，不能被其他数整除的数叫素数
 * 
 * @author Sylvain Hallé
 */
public class PrimeNumbers {
    public static void main(String[] args) {
        SequentialWorkflow<Integer, Integer> w = new SequentialWorkflow<Integer, Integer>(new PrimeMap(), // Mapper
                new PrimeReduce(), // Reducer
                new PrimeCollector() // Input collector
        );
        // Run the workflow
        InCollector<Integer, Integer> results = w.run();
        // Iterate over InCollector to display results
        System.out.println(results);
    }

    /**
     * Create the initial tuples to be sent to the map-reduce job. This
     * collector will contain tuples of the form (<i>x</i>,<i>y</i>), with
     * <i>x</i> and <i>y</i> all combination of integers between 1 and 1,000.
     */
    private static class PrimeCollector extends Collector<Integer, Integer> {
        /* package */ PrimeCollector() {
            super();
            for (int i = 1; i <= 1000; i++) {
                for (int j = 1; j <= 1000; j++) {
                    Tuple<Integer, Integer> t = new Tuple<Integer, Integer>(new Integer(i), new Integer(j));
                    super.collect(t);
                }
            }
        }
    }

    /**
     * Implementation of the mapper.
     * <ol>
     * <li>Input: a tuple (<i>x</i>,<i>y</i>), with <i>x</i> and <i>y</i>
     * integers</li>
     * <li>Output: the same tuple (<i>x</i>,<i>y</i>), only if <i>y</i> divides
     * <i>x</i> (and nothing otherwise)</li>
     * </ol>
     * 
     * @author Sylvain Hallé
     *
     */
    private static class PrimeMap implements Mapper<Integer, Integer> {
        @Override
        public void map(OutCollector<Integer, Integer> out, Tuple<Integer, Integer> t) {
            int i = t.getKey().intValue();
            int j = t.getValue().intValue();
            //有整除关系就记录下来
            if (i % j == 0)
                out.collect(t);
        }
    }

    /**
     * Implementation of the reducer.
     * <ol>
     * <li>Input: tuple (<i>x</i>,<i>y</i>), with <i>x</i> and <i>y</i> integers
     * and <i>y</i> divides <i>x</i></li>
     * <li>Output: the tuple (<i>x</i>,1), if the only values seen for <i>y</i>
     * are 1 and <i>x</i> (and nothing otherwise)</li>
     * </ol>
     * 
     * @author Sylvain Hallé
     *
     */
    private static class PrimeReduce implements Reducer<Integer, Integer> {

        /**
         *
         * @param out A {@link OutCollector} that will be used to write output
         *        tuples
         * @param key in中所有的key都是一样的，其值就是此参数
         * @param in 此集合中所有tuple的key都是一样
         */
        @Override
        public void reduce(OutCollector<Integer, Integer> out, Integer key, InCollector<Integer, Integer> in) {
            boolean ok = true;
            int i = key.intValue();
            while (in.hasNext() && ok) {
                Tuple<Integer, Integer> t = in.next();
                int j = t.getValue().intValue();
                // 只要j不是1和i 那么 i就是素数
                if (j != 1 && j != i)
                    ok = false;
            }
            if (ok)
                out.collect(new Tuple<Integer, Integer>(key, new Integer(1)));
        }
    }
}
