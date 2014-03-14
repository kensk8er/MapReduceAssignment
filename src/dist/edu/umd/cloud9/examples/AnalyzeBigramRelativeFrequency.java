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

import edu.umd.cloud9.io.PairOfStrings;
import edu.umd.cloud9.io.PairOfWritables;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;

import java.io.*;
import java.util.*;

public class AnalyzeBigramRelativeFrequency {
	public static void main(String[] args) {
		if (args.length != 1) {
			System.out.println("usage: [input-path]");
			System.exit(-1);
		}

		System.out.println("input path: " + args[0]);

		List<PairOfWritables<PairOfStrings, FloatWritable>> pairs;
		try {
			pairs = readDirectory(new Path(args[0]));

			List<PairOfWritables<PairOfStrings, FloatWritable>> list1 = new ArrayList<PairOfWritables<PairOfStrings, FloatWritable>>();
			List<PairOfWritables<PairOfStrings, FloatWritable>> list2 = new ArrayList<PairOfWritables<PairOfStrings, FloatWritable>>();
            List<PairOfWritables<PairOfStrings, FloatWritable>> list3 = new ArrayList<PairOfWritables<PairOfStrings, FloatWritable>>();

            Float probability1 = 0.0032f; // given value
            Float probability2 = 0.0f;
            Float probability3 = 0.0f;
            Float probability4 = 0.0f;

			for (PairOfWritables<PairOfStrings, FloatWritable> p : pairs) {
				PairOfStrings bigram = p.getLeftElement();

				if (bigram.getLeftElement().equals("romeo")) {
					list1.add(p);
				}
				if (bigram.getLeftElement().equals("is")) {
					list2.add(p);
				}
                if (bigram.getLeftElement().equals("the")) {
                    list3.add(p);
                }
			}

            // sort for romeo
            Collections.sort(list1,
                    new Comparator<PairOfWritables<PairOfStrings, FloatWritable>>() {
                        public int compare(PairOfWritables<PairOfStrings, FloatWritable> e1,
                                           PairOfWritables<PairOfStrings, FloatWritable> e2) {
                            if (e1.getRightElement().compareTo(e2.getRightElement()) == 0) {
                                return e1.getLeftElement().compareTo(e2.getLeftElement());
                            }

                            return e2.getRightElement().compareTo(e1.getRightElement());
                        }
                    });

            int i = 0;
            Boolean doneLoop = false;
            for (PairOfWritables<PairOfStrings, FloatWritable> p : list1) {
                PairOfStrings bigram = p.getLeftElement();
                System.out.println(bigram + "\t" + p.getRightElement());
                i++;

                if (bigram.getRightElement().equals("is")) {
                    probability2 = Float.valueOf(p.getRightElement().toString());
                    doneLoop = true;
                }

                if (i > 5) {
                    if (doneLoop == true) {
                        break;
                    }
                }
            }

            Collections.sort(list2,
                    new Comparator<PairOfWritables<PairOfStrings, FloatWritable>>() {
                        public int compare(PairOfWritables<PairOfStrings, FloatWritable> e1,
                                           PairOfWritables<PairOfStrings, FloatWritable> e2) {
                            if (e1.getRightElement().compareTo(e2.getRightElement()) == 0) {
                                return e1.getLeftElement().compareTo(e2.getLeftElement());
                            }

                            return e2.getRightElement().compareTo(e1.getRightElement());
                        }
                    });

            i = 0;
            doneLoop = false;
            for (PairOfWritables<PairOfStrings, FloatWritable> p : list2) {
                PairOfStrings bigram = p.getLeftElement();
                i++;

                if (bigram.getRightElement().equals("the")) {
                    probability3 = Float.valueOf(p.getRightElement().toString());
                    doneLoop = true;
                }
                if (doneLoop == true) {
                    break;
                }
            }

            Collections.sort(list3,
                    new Comparator<PairOfWritables<PairOfStrings, FloatWritable>>() {
                        public int compare(PairOfWritables<PairOfStrings, FloatWritable> e1,
                                           PairOfWritables<PairOfStrings, FloatWritable> e2) {
                            if (e1.getRightElement().compareTo(e2.getRightElement()) == 0) {
                                return e1.getLeftElement().compareTo(e2.getLeftElement());
                            }

                            return e2.getRightElement().compareTo(e1.getRightElement());
                        }
                    });

            i = 0;
            doneLoop = false;
            for (PairOfWritables<PairOfStrings, FloatWritable> p : list3) {
                PairOfStrings bigram = p.getLeftElement();
                i++;

                if (bigram.getRightElement().equals("king")) {
                    probability4 = Float.valueOf(p.getRightElement().toString());
                    doneLoop = true;
                }
                if (doneLoop == true) {
                    break;
                }
            }

            System.out.println("\np(romeo) = " + probability1);
            System.out.println("p(is|romeo) = " + probability2);
            System.out.println("p(the|is) = " + probability3);
            System.out.println("p(king|the) = " + probability4 + "\n");
            System.out.println("Bigram probability of \"romeo is the king\" is: "
                    + probability1 * probability2 * probability3 * probability4);

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
	 * @throws IOException
	 */
	private static List<PairOfWritables<PairOfStrings, FloatWritable>> readDirectory(Path path)
			throws IOException {

		File dir = new File(path.toString());
		ArrayList<PairOfWritables<PairOfStrings, FloatWritable>> relativeFrequencies = new ArrayList<PairOfWritables<PairOfStrings, FloatWritable>>();
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
			String firstWord;
			String secondWord;
			String frequency;

			// iterate through every line in the file
			while ((rLine = results.readLine()) != null) {
				rToken = new StringTokenizer(rLine);
				// extract the meaningful information
				firstWord = rToken.nextToken();
				//remove leading ( and trailing ,
				firstWord = firstWord.substring(1, firstWord.length() - 1);
				secondWord = rToken.nextToken();
				//remove trailing )
				secondWord = secondWord.substring(0, secondWord.length() - 1);
				frequency = rToken.nextToken();

				relativeFrequencies.add(new PairOfWritables<PairOfStrings, FloatWritable>(
						new PairOfStrings(firstWord, secondWord), new FloatWritable(Float
								.parseFloat(frequency))));

			}
			if (bigramFile != null)
				bigramFile.close();
		}

		return relativeFrequencies;

	}
}
