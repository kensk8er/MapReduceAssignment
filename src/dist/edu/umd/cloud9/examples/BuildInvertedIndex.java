package edu.umd.cloud9.examples;

import edu.umd.cloud9.io.ArrayListWritable;
import edu.umd.cloud9.io.PairOfInts;
import edu.umd.cloud9.io.PairOfWritables;
import edu.umd.cloud9.util.EntryObject2IntFrequencyDistribution;
import edu.umd.cloud9.util.Object2IntFrequencyDistribution;
import edu.umd.cloud9.util.PairOfObjectInt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * Created by kensk8er on 14/03/2014.
 *
 */
public class BuildInvertedIndex  extends Configured implements Tool {
    private static final Logger sLogger = Logger.getLogger(BuildInvertedIndex.class);

    /**
     *  Mapper: emits (token, 1) for every previousWord occurrence
     *
     */
    private static class MyMapper extends MapReduceBase implements
            Mapper<LongWritable, Text, Text, PairOfInts> {

        /**
         * reuse objects to save overhead of object creation
         */
        private static Text WORD = new Text();
        private static Object2IntFrequencyDistribution<String> COUNTS = new EntryObject2IntFrequencyDistribution<String>();

        /**
         * Mapping function. This takes the text input, converts it into a String which is split into
         * words, then each of the words is mapped to the OutputCollector with a count of
         * one.
         *
         * @param key Input key, not used in this example
         * @param value A line of input Text taken from the data
         * @param output Map from each previousWord (Text) to its count (IntWritable)
         */
        public void map(LongWritable key, Text value, OutputCollector<Text, PairOfInts> output,
                        Reporter reporter) throws IOException {
            //Convert input previousWord into String and tokenize to find words
            String line = ((Text) value).toString();
            StringTokenizer itr = new StringTokenizer(line);
            COUNTS.clear();
            //For each previousWord, map it to a count of one. Duplicate words will be counted
            //in the reduce phase.
            while (itr.hasMoreTokens()) {
                String term = itr.nextToken();
                if (term != null && term.length() > 0) {
                    COUNTS.increment(term);
                }
            }

            for (PairOfObjectInt<String> count : COUNTS) {
                WORD.set(count.getLeftElement());
                PairOfInts wordCount = new PairOfInts((int) key.get(), count.getRightElement());
                output.collect(WORD, wordCount);
            }
        }
    }

    /**
     * Reducer: sums up all the counts
     *
     */
    private static class MyReducer extends MapReduceBase implements
            Reducer<Text, PairOfInts, Text, PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>> {

        /**
         * define variables
         */
        private static IntWritable DF = new IntWritable();

        /**
         *  @param key The Text previousWord
         *  @param values An iterator over the values associated with this previousWord
         *  @param output Map from each previousWord (Text) to its count (IntWritable)
         *  @param reporter Used to report progress
         */
        public void reduce(Text key, Iterator<PairOfInts> values,
                           OutputCollector<Text, PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>> output,
                           Reporter reporter) throws IOException {

            int df = 0;
            ArrayListWritable<PairOfInts> postings = new ArrayListWritable<PairOfInts>();
            while (values.hasNext()) {
                postings.add(values.next().clone());
                df++;
            }

            Collections.sort(postings);
            DF.set(df);
            PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>> invertedIndex
                    = new PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>(DF, postings);
            output.collect(key, invertedIndex);
        }
    }

    /**
     * Creates an instance of this tool.
     */
    public BuildInvertedIndex() {
    }

    /**
     *  Prints argument options
     * @return
     */
    private static int printUsage() {
        System.out.println("usage: [input-path] [output-path] [num-mappers] [num-reducers]");
        ToolRunner.printGenericCommandUsage(System.out);
        return -1;
    }

    /**
     * Runs this tool.
     */
    public int run(String[] args) throws Exception {
        if (args.length != 4) {
            printUsage();
            return -1;
        }

        String inputPath = args[0];
        String outputPath = args[1];

        int mapTasks = Integer.parseInt(args[2]);
        int reduceTasks = Integer.parseInt(args[3]);

        sLogger.info("Tool: DemoWordCount");
        sLogger.info(" - input path: " + inputPath);
        sLogger.info(" - output path: " + outputPath);
        sLogger.info(" - number of mappers: " + mapTasks);
        sLogger.info(" - number of reducers: " + reduceTasks);

        JobConf conf = new JobConf(BuildInvertedIndex.class);
        conf.setJobName("DemoWordCount");

        conf.setNumMapTasks(mapTasks);
        conf.setNumReduceTasks(reduceTasks);

        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));
        FileOutputFormat.setCompressOutput(conf, false);

        /**
         *  Note that these must match the Class arguments given in the mapper
         */
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(PairOfInts.class);

        conf.setMapperClass(MyMapper.class);
        conf.setReducerClass(MyReducer.class);

        // Delete the output directory if it exists already
        Path outputDir = new Path(outputPath);
        FileSystem.get(outputDir.toUri(), conf).delete(outputDir, true);

        long startTime = System.currentTimeMillis();
        JobClient.runJob(conf);
        sLogger.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
                + " seconds");

        return 0;
    }

    /**
     * Dispatches command-line arguments to the tool via the
     * <code>ToolRunner</code>.
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new BuildInvertedIndex(), args);
        System.exit(res);
    }
}
