/*
 * Cloud9: A MapReduce Library for Hadoop
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package edu.umd.cloud9.examples;

import edu.umd.cloud9.io.ArrayListWritable;
import edu.umd.cloud9.io.PairOfInts;
import edu.umd.cloud9.io.PairOfWritables;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

public class LookupPostings {
	public static void main(String[] args) {
		if (args.length != 1) {
			System.out.println("usage: [input-path]");
			System.exit(-1);
		}

		System.out.println("input path: " + args[0]);

        List<PairOfWritables<Text, PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>>> pairs;
        Text key = new Text();
        PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>> value =
                new PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>();

        try {
			pairs = readDirectory(new Path(args[0]));
            HashMap<String, HashMap<Integer, Integer>> histGram
                    = new HashMap<String, HashMap<Integer, Integer>>();
            Integer docno = 0;

			for (PairOfWritables<Text, PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>> p : pairs) {
                String term = p.getLeftElement().toString();

                if (term.equals("king") || term.equals("macbeth") || term.equals("juliet")) {
                    PairOfWritables invertedIndex = p.getRightElement();

                    ArrayListWritable<PairOfInts> entries = (ArrayListWritable) invertedIndex.getRightElement();
                    for (PairOfInts entry : entries) {
                        Integer tf = entry.getRightElement();
                        if (histGram.containsKey(term)) {
                            int count = histGram.get(term).containsKey(tf) ? histGram.get(term).get(tf) : 0;
                            HashMap<Integer, Integer> temp = new HashMap<Integer, Integer>(histGram.get(term));
                            temp.put(tf, count + 1);
                            histGram.put(term, temp);
                        } else {
                            HashMap<Integer, Integer> temp = new HashMap<Integer, Integer>();
                            temp.put(tf, 1);
                            histGram.put(term, temp);
                        }
                    }
                }
                if (term.equals("martino")) {
                    PairOfWritables invertedIndex = p.getRightElement();
                    ArrayListWritable<PairOfInts> postings = (ArrayListWritable<PairOfInts>) invertedIndex.getRightElement();
                    if (postings.size() == 1) {
                        docno = postings.get(0).getLeftElement();
                    } else {
                        System.out.println("The number of line containing \"martino\" is not one!");
                        System.exit(-1);
                    }
                }
			}

            System.out.println("<hist gram of each work>");
            for (Map.Entry<String, HashMap<Integer, Integer>> entry : histGram.entrySet()) {
                String term = entry.getKey();
                System.out.println("term: " + term);

                for (Map.Entry<Integer, Integer> frequency : entry.getValue().entrySet()) {
                    System.out.println(frequency.getKey() + ": " + frequency.getValue());
                }
                System.out.println();
            }
            System.out.println();
            System.out.println("the docno of the line which contains \"martino\": " + docno.toString());

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Reads in the bigram relative frequency count file
	 *
	 * @param path
	 * @return
	 * @throws java.io.IOException
	 */
	private static List<PairOfWritables<Text, PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>>> readDirectory(Path path)
			throws IOException {

		File dir = new File(path.toString());

        PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>> value =
                new PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>();

        ArrayListWritable<PairOfWritables<Text, PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>>> returnList
                = new ArrayListWritable<PairOfWritables<Text, PairOfWritables<IntWritable,ArrayListWritable<PairOfInts>>>>();

        for (File child : dir.listFiles()) {
			if (".".equals(child.getName()) || "..".equals(child.getName())) {
				continue; // Ignore the self and parent aliases.
			}
			FileInputStream bigramFile = null;

			bigramFile = new FileInputStream(child.toString());

			// Read in the file
			DataInputStream resultsStream = new DataInputStream(bigramFile);
			BufferedReader results = new BufferedReader(new InputStreamReader(resultsStream));

			StringTokenizer rToken;
			String rLine;
			String invertedIndex;
            String df;
            String posting;

            // iterate through every line in the file
			while ((rLine = results.readLine()) != null) {
                ArrayListWritable<PairOfInts> postings = new ArrayListWritable<PairOfInts>();
				rToken = new StringTokenizer(rLine, "\t\n\r\f");
				// extract the meaningful information
				Text key = new Text(rToken.nextToken());
				invertedIndex = rToken.nextToken();
                rToken = new StringTokenizer(invertedIndex, "[");
                df = rToken.nextToken();
                df = df.substring(1, df.length() - 2);
                posting = rToken.nextToken();
                posting = posting.substring(0, posting.length() - 2);
                rToken = new StringTokenizer(posting, "(");
                while (rToken.hasMoreTokens()) {
                    StringTokenizer pToken = new StringTokenizer(rToken.nextToken(), ",");
                    String docno = pToken.nextToken();
                    String tf = pToken.nextToken();
                    tf = tf.substring(1, tf.length() - 1);
                    postings.add(new PairOfInts(Integer.valueOf(docno), Integer.valueOf(tf)));
                }

                IntWritable dfWritable = new IntWritable(Integer.valueOf(df));
                PairOfWritables rightElement =
                        new PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>(dfWritable, postings);
                PairOfWritables<Text, PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>> addValue
                        = new PairOfWritables(key, rightElement);
                returnList.add(addValue);
			}
			if (bigramFile != null)
				bigramFile.close();
		}

		return returnList;

	}
}
