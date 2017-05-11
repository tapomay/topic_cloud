package cs185c.project;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.DistributedLDAModel;
import org.apache.spark.mllib.clustering.LDA;
import org.apache.spark.mllib.clustering.LDAModel;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder;

import scala.Tuple2;

/**
 * Consolidating relevant examples from:
 * https://github.com/apache/spark/tree/master/examples/src/main/java/org/apache/spark/examples
 * 
 * @author tadey
 *
 */
public class BasicSparkApp {
	private static final Pattern SPACE = Pattern.compile(" ");
	private static final int STOP_WORD_COUNT = 20;
	private static final int TOPIC_COUNT = 10;

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		BasicSparkApp app = new BasicSparkApp();
//		app.wordCount("problem.txt");
//		app.testLDANum("sample_lda_data.txt");
		app.testLDAStr("problem.txt");
	}

	public void wordCount(String filepath) {
		Builder builder = SparkSession.builder();
		builder = builder.master("local").appName("JavaWordCount");
		builder = builder.config("spark.sql.warehouse.dir", "file:///C:/Users/tadey/workspace/cs185c-project");
		SparkSession spark = builder.getOrCreate();

		JavaRDD<String> lines = spark.read().textFile(filepath).javaRDD();

		JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());

		JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

		JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

		List<Tuple2<String, Integer>> output = counts.collect();
		for (Tuple2<?, ?> tuple : output) {
			System.out.println(tuple._1() + ": " + tuple._2());
		}
		spark.stop();
	}

	public void testLDANum(String filepath) {
		SparkConf conf = new SparkConf().setAppName("JavaKLatentDirichletAllocationExample");
		conf.setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		// $example on$
		// Load and parse the data
//		String path = "data/mllib/sample_lda_data.txt";
		JavaRDD<String> data = jsc.textFile(filepath);
		JavaRDD<Vector> parsedData = data.map(s -> {
			String[] sarray = s.trim().split(" ");
			double[] values = new double[sarray.length];
			for (int i = 0; i < sarray.length; i++) {
				values[i] = Double.parseDouble(sarray[i]);
			}
			return Vectors.dense(values);
		});
		// Index documents with unique IDs
		JavaPairRDD<Long, Vector> corpus = JavaPairRDD.fromJavaRDD(parsedData.zipWithIndex().map(Tuple2::swap));
		corpus.cache();

		// Cluster the documents into three topics using LDA
		LDAModel ldaModel = new LDA().setK(3).run(corpus);

		// Output topics. Each is a distribution over words (matching word count
		// vectors)
		System.out.println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize() + " words):");
		Matrix topics = ldaModel.topicsMatrix();
		for (int topic = 0; topic < 3; topic++) {
			System.out.print("Topic " + topic + ":");
			for (int word = 0; word < ldaModel.vocabSize(); word++) {
				System.out.print(" " + topics.apply(word, topic));
			}
			System.out.println();
		}

		ldaModel.save(jsc.sc(), "target/org/apache/spark/JavaLatentDirichletAllocationExample/LDAModel");
		DistributedLDAModel sameModel = DistributedLDAModel.load(jsc.sc(),
				"target/org/apache/spark/JavaLatentDirichletAllocationExample/LDAModel");
		// $example off$

		jsc.stop();
	}
	
	public void testLDAStr(String filepath) throws Exception {
		/**
		 *	https://gist.github.com/jkbradley/ab8ae22a8282b2c8ce33
		 * 
		 */
		SparkConf conf = new SparkConf().setAppName("JavaKLatentDirichletAllocationExample");
		conf.setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(conf);

//		String path = "data/mllib/sample_lda_data.txt";
		JavaRDD<String> data = jsc.textFile(filepath);
//		jsc.wholeTextFiles(filepath).values(); //for directory
		
		JavaRDD<String> dataSplit = data.flatMap(filedata -> Arrays.asList(filedata.toLowerCase().split(" ")).iterator());
		
		Function<String, Boolean> filterFunc = (token -> {
			Boolean cond1 = token.length() > 3;
			Boolean cond2 = true;
			for (Character c : token.toCharArray()) {
				cond2 = cond2 && java.lang.Character.isLetter(c);
			}
			Boolean ret = cond1 && cond2;
			return ret;
		});
		
		JavaRDD<String> dataTokens = dataSplit.filter(filterFunc);
		
		
//		val termCounts: Array[(String, Long)] =
//				  tokenized.flatMap(_.map(_ -> 1L)).reduceByKey(_ + _).collect().sortBy(-_._2)
		JavaPairRDD<String, Integer> wordCounts = dataTokens.mapToPair(word -> new Tuple2<>(word, 1)).reduceByKey((a, b) -> a + b);
		List<Tuple2<String,Integer>> collect = wordCounts.collect();

		Comparator<Tuple2<String,Integer>> comp = new Comparator<Tuple2<String,Integer>>() {
			
			@Override
			public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
				return o2._2() - o1._2(); //descending
			}
		};

		//		Collections.sort(collect, comp); //unsupported since immutable list
		List<Tuple2<String,Integer>> collectSorted = collect.stream().sorted(comp).collect(Collectors.toList());
		
		int vocabCount = collectSorted.size() - STOP_WORD_COUNT;
		
		if (vocabCount <= STOP_WORD_COUNT) {
			jsc.close();
			throw new Exception("Doc too small: " + collectSorted.size());
		}

		Map<String, Integer> vocabIndex = new HashMap<String, Integer>();
		
		for (int i = 0; i < vocabCount; i++) {
			vocabIndex.put(collectSorted.get(i)._1(), i);
		}
		
		System.out.println(vocabIndex);
		
		JavaRDD<String[]> dataFilesRDD = data.map(filedata -> filedata.toLowerCase().split(" "));
		JavaPairRDD<String[],Long> zipWithIndex = dataFilesRDD.zipWithIndex();
		
		Function<Tuple2<String[], Long>, Tuple2<Long, Vector>> vectorizerFunc = (tup -> {
			String[] tokens = tup._1();
			Long docId = tup._2();
			Map<Integer, Double> countsMap = new HashMap<>();
			for (String token : tokens) {
				Integer tokenIdx = vocabIndex.get(token);
				if (null != tokenIdx) {
					Double count = countsMap.get(tokenIdx);
					if (null == count) {
						count = 0d;
						countsMap.put(tokenIdx, count);
					}
					count += 1;
					countsMap.put(tokenIdx, count);
				}
				else {
					System.out.println("Token not found in index: " + tokenIdx);
				}
			}
			List<Tuple2<Integer, Double>> tupleList = countsMap.entrySet().stream().map(entry -> new Tuple2<Integer, Double>(entry.getKey(), entry.getValue())).collect(Collectors.toList());
			Vector v = Vectors.sparse(vocabCount, tupleList);
			return new Tuple2<Long, Vector>(docId, v);
		});
		
		JavaPairRDD<Long,Vector> corpus =  JavaPairRDD.fromJavaRDD(zipWithIndex.map(vectorizerFunc));
		
		LDAModel ldaModel = new LDA().setK(TOPIC_COUNT).run(corpus);

		Tuple2<int[],double[]>[] describeTopics = ldaModel.describeTopics(10);
		int topic = 0;

		Map<Integer, String> revVocabIndex = new HashMap<Integer, String>();
		vocabIndex.forEach(new BiConsumer<String, Integer>(){
			@Override
			public void accept(String t, Integer u) {
				revVocabIndex.put(u, t);
			}
		});

		for (Tuple2<int[], double[]> topicDescription : describeTopics) {
			System.out.print("\nTopic " + topic + ":");
			int[] tokenIndices = topicDescription._1();
			double[] tokenWeights = topicDescription._2();
			for (int i = 0; i < tokenWeights.length; i++) {
				System.out.print(String.format("\n%d: %s : %f", tokenIndices[i], revVocabIndex.get(tokenIndices[i]), tokenWeights[i]));
			}
			topic++;
		}
		// Output topics. Each is a distribution over words (matching word count
		// vectors)
		
		System.out.println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize() + " words):");
		Matrix topics = ldaModel.topicsMatrix();
		for (topic = 0; topic < 3; topic++) {
			System.out.print("Topic " + topic + ":");
			for (int word = 0; word < ldaModel.vocabSize(); word++) {
				System.out.print(" " + topics.apply(word, topic));
			}
			System.out.println();
		}

		jsc.close();

	
	//   vocab: Map term -> term index
//		val vocab: Map[String, Int] = vocabArray.zipWithIndex.toMap
//
//		// Convert documents into term count vectors
//		val documents: RDD[(Long, Vector)] =
//		  tokenized.zipWithIndex.map { case (tokens, id) =>
//		    val counts = new mutable.HashMap[Int, Double]()
//		    tokens.foreach { term =>
//		      if (vocab.contains(term)) {
//		        val idx = vocab(term)
//		        counts(idx) = counts.getOrElse(idx, 0.0) + 1.0
//		      }
//		    }
//		    (id, Vectors.sparse(vocab.size, counts.toSeq))
//		  }
		
		
		
//		JavaRDD<Vector> parsedData = data.map(s -> {
//			String[] sarray = s.trim().split(" ");
//			double[] values = new double[sarray.length];
//			for (int i = 0; i < sarray.length; i++) {
//				values[i] = Double.parseDouble(sarray[i]);
//			}
//			return Vectors.dense(values);
//		});
//		// Index documents with unique IDs
//		JavaPairRDD<Long, Vector> corpus = JavaPairRDD.fromJavaRDD(parsedData.zipWithIndex().map(Tuple2::swap));
//		corpus.cache();
//
//		// Cluster the documents into three topics using LDA
//		LDAModel ldaModel = new LDA().setK(3).run(corpus);
//
//		// Output topics. Each is a distribution over words (matching word count
//		// vectors)
//		System.out.println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize() + " words):");
//		Matrix topics = ldaModel.topicsMatrix();
//		for (int topic = 0; topic < 3; topic++) {
//			System.out.print("Topic " + topic + ":");
//			for (int word = 0; word < ldaModel.vocabSize(); word++) {
//				System.out.print(" " + topics.apply(word, topic));
//			}
//			System.out.println();
//		}
//
//		ldaModel.save(jsc.sc(), "target/org/apache/spark/JavaLatentDirichletAllocationExample/LDAModel");
//		DistributedLDAModel sameModel = DistributedLDAModel.load(jsc.sc(),
//				"target/org/apache/spark/JavaLatentDirichletAllocationExample/LDAModel");
//		// $example off$

	}
}
