package com.bow.camptotheca.mr;

import java.io.*;
import java.util.*;

/**
 * Demonstration of MapReduce processing with MrSim. This program compute the
 * list of "<i>n</i>-anagrams" from a dictionary file. An <i>n</i>-anagram is a
 * set of letters that can be used to form <i>n</i> or more dictionary words.
 * This is a slightly modified re-implementation of the <a href=
 * "http://code.google.com/p/hadoop-map-reduce-examples/wiki/Anagram_Example">
 * example provided by Hadoop</a>.
 *
 *
 * 例如 carest,carets,cartes,caster,caters 这些单词只是字母顺序不一样，这个demo就是把字符一样的单词全部找出
 * 
 * @author Sylvain Hallé
 *
 */
public class Anagrams {

    public static void main(String[] args) {
        int n = 8; // We look for n-anagrams
        InCollector<String, String> data = new WordCollector("data/English-Dictionary.txt");
        SequentialWorkflow<String, String> w = new SequentialWorkflow<String, String>(new AnagramMap(), // Mapper
                new AnagramReduce(n), // Reducer
                data // Input reader
        );
        InCollector<String, String> results = w.run();
        System.out.println("Out of " + data.count() + " words, there are " + results.count() + " " + n + "-anagrams");
        System.out.println(results);
    }

    /**
     * Create the initial tuples to be sent to the map-reduce, which contains
     * tuples of the form (<i>w</i>, ""), for <i>w</i> each word taken from the
     * dictionary file given as parameter.
     */
    private static class WordCollector extends Collector<String, String> {
        /**
         * Creates a WordCollector from an input text file. Each line in the
         * file should contain a single expression.
         * 
         * @param filename The file to read
         */
        /* package */ WordCollector(String filename) {
            super();
            try {
                InputStreamReader reader = new InputStreamReader(
                        WordCollector.class.getClassLoader().getResourceAsStream(filename));
                BufferedReader input = new BufferedReader(reader);
                try {
                    String line = null;
                    while ((line = input.readLine()) != null) {
                        super.collect(new Tuple<String, String>(line, ""));
                    }
                } finally {
                    input.close();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    /**
     * Implementation of the mapper.
     * <ol>
     * <li>Input: a tuple (<i>w</i>,""), with <i>w</i> a word</li>
     * <li>Output: the tuple (<i>w'</i>,<i>w</i>), where <i>w'</i> contains the
     * letters of <i>w</i> sorted in alphabetical order</li>
     * </ol>
     * 
     * @author Sylvain Hallé
     *
     */
    private static class AnagramMap implements Mapper<String, String> {


        @Override
        public void map(OutCollector<String, String> out, Tuple<String, String> t) {
            // 去除每个单词，给字符重新排序，排好后将此tuple收集起来
            String word = t.getKey();
            char[] wordChars = word.toCharArray();
            Arrays.sort(wordChars);
            String sortedWord = new String(wordChars);
            out.collect(new Tuple<String, String>(sortedWord, word));
        }
    }

    /**
     * Implementation of the reducer.
     * <ol>
     * <li>Input: tuples (<i>w'</i>,<i>w</i>), with <i>w'</i> a sorted list of
     * letters and <i>y</i> all the words from the dictionary you can make from
     * <i>w'</i></li>
     * <li>Output: the tuple (<i>w'</i>,<i>w''</i>), with <i>w''</i> a
     * comma-separated list of dictionary words, only if at least <i>n</i> words
     * are present in the input (and nothing otherwise)</li>
     * </ol>
     * With <i>n</i>=2, we have the regular definition of an anagram.
     * 
     * @author Sylvain Hallé
     *
     */
    private static class AnagramReduce implements Reducer<String, String> {

        private int m_numWords = 2;

        /**
         * Constructs a reducer and sets the number of word occurrences required
         * to retain a tuple. This number is the <i>n</i> in the description
         * above.
         * 
         * @param n Number of word occurrences
         */
        /* package */ AnagramReduce(int n) {
            m_numWords = n;
        }

        @Override
        public void reduce(OutCollector<String, String> out, String key, InCollector<String, String> in) {
            // 将每个单词用','连接起来
            String word_list = "";
            int num_words = 0;
            while (in.hasNext()) {
                num_words++;
                Tuple<String, String> t = in.next();
                word_list += (num_words > 1 ? "," : "") + t.getValue();
            }
            // InCollector 中有一个以上的单词就记录下来
            if (num_words >= m_numWords){
                out.collect(new Tuple<String, String>(key, word_list));
            }

        }
    }

}
