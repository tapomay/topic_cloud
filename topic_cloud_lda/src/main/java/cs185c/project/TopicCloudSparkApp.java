package cs185c.project;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.BiConsumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.clustering.LDA;
import org.apache.spark.mllib.clustering.LDAModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka09.ConsumerStrategies;
import org.apache.spark.streaming.kafka09.KafkaUtils;
import org.apache.spark.streaming.kafka09.LocationStrategies;

import com.fasterxml.jackson.databind.ObjectMapper;

import scala.Tuple2;

/**
 *
 */

public final class TopicCloudSparkApp {
	private static final ObjectMapper objectMapper = new ObjectMapper();
	private static final Logger logger = Logger.getLogger("TopicCloudSparkApp");
	private static final int STOP_WORD_COUNT = 20;
	private static final int TOPIC_COUNT = 10;
	
	private static transient JavaStreamingContext jssc;
	
	public static JavaPairDStream<String, String> buildKafkaDStream(JavaStreamingContext jssc, String docTopic, String broker, String consumerGroup) {
		final Pattern topicPattern = Pattern.compile(docTopic, Pattern.CASE_INSENSITIVE);
		Map<String, Object> kafkaParams = new HashMap<String, Object>();
		kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		kafkaParams.put("bootstrap.servers", broker);
		kafkaParams.put("group.id", consumerGroup);
		kafkaParams.put("enable.auto.commit", true);
		
		final JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(jssc,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>SubscribePattern(topicPattern, kafkaParams));

		// pull values out of ConsumerRecords
		JavaPairDStream<String, String> keyValuePairs = messages
				.mapToPair(consumerRecord -> new Tuple2<String, String>(consumerRecord.key(), consumerRecord.value()));
		
		return keyValuePairs;
	}

	public static void publishDocName(String docName, String broker, String metaTopic) {
		Properties producerProps = new Properties();
		producerProps.put("bootstrap.servers", broker);
		producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(
				producerProps);
		String msg = String.format("New doc received: %s", docName);
		ProducerRecord<String, String> rec = new ProducerRecord<String, String>(metaTopic, msg);
		producer.send(rec);
		producer.close();
	}
	
	public static void dispatchLDA(JavaStreamingContext jssc, List<JavaRDD<String>> rddList) throws Exception {
		
		System.out.println("Current batch size: " + rddList.size());
		//Combine all current RDDs
		JavaRDD<String> dataRDD = jssc.sparkContext().emptyRDD(); //fresh RDD required as new object for unpersist at the end
		for (int i = 0; i < rddList.size(); i++) {
			JavaRDD<String> batchRDD = rddList.get(i);
			System.out.println("drdd: " + dataRDD);
			System.out.println("brdd: " + batchRDD);
			dataRDD = dataRDD.union(batchRDD);
		}
		dataRDD = rddList.get(1);
		//Each row in RDD should be a file
		//Combine all files to RDD of words
		JavaRDD<String> dataSplit = dataRDD.flatMap(filedata -> Arrays.asList(filedata.toLowerCase().split(" ")).iterator());
		
		Function<String, Boolean> filterFunc = (token -> {
			Boolean cond1 = token.length() > 3;
			Boolean cond2 = true;
			for (Character c : token.toCharArray()) {
				cond2 = cond2 && java.lang.Character.isLetter(c);
			}
			Boolean ret = cond1 && cond2;
			return ret;
		});
		
		// filtered RDD of words
		JavaRDD<String> dataTokens = dataSplit.filter(filterFunc);
		
		// word frequency
		JavaPairRDD<String, Integer> wordCounts = dataTokens.mapToPair(word -> new Tuple2<>(word, 1)).reduceByKey((a, b) -> a + b);
		List<Tuple2<String,Integer>> collect = wordCounts.collect();

		Comparator<Tuple2<String,Integer>> comp = new Comparator<Tuple2<String,Integer>>() {
			
			@Override
			public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
				return o2._2() - o1._2(); //descending
			}
		};

		// words sorted by frequency
		List<Tuple2<String,Integer>> collectSorted = collect.stream().sorted(comp).collect(Collectors.toList());
		
		int vocabCount = collectSorted.size() - STOP_WORD_COUNT;
		
		if (vocabCount <= STOP_WORD_COUNT) {
			throw new Exception("Doc too small: " + collectSorted.size());
		}

		Map<String, Integer> vocabIndex = new HashMap<String, Integer>();
		
		for (int i = 0; i < vocabCount; i++) {
			vocabIndex.put(collectSorted.get(i)._1(), i);
		}
		
		System.out.println(vocabIndex);
		
		//Combine all files to RDD of word arrays
		JavaRDD<String[]> dataFilesRDD = dataRDD.map(filedata -> filedata.toLowerCase().split(" "));
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
		
		// vectorize
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
		
		//Ensure unpersist on combined RDD
		dataRDD.unpersist();
	}
	
	public static void main(String[] args) {
//		if (args.length < 6) {
//			System.err.println("Usage: TopicCloudSparkApp <broker> <master> <in-topic> <out-topic> <cg> <interval>");
//			System.err.println("eg: TopicCloudSparkApp localhost:9092 local/localhost:sparkport docs topiccloud mycg 5000");
//			System.exit(1);
//		}
//
//		// set variables from command-line arguments
//		final String broker = args[0];
//		final String master = args[1];
//		final String docTopic = args[2];
//		final String dataTopic = args[3];
//		final String metaTopic = args[4];
//		final String consumerGroup = args[5];
//		final long interval = Long.parseLong(args[6]);

		String broker = "localhost:9092";
		String master = "local";
		String docTopic = "docs";
		String dataTopic = "tcdata";
		String metaTopic = "tcmeta";
		
		String consumerGroup = "mycg";
		long intervalMs = 5000;

		
		// initialize the streaming context
		jssc = new JavaStreamingContext(master, "StockSparkApp", new Duration(intervalMs));
//		Logger.getRootLogger().setLevel(Level.ERROR);
		
		JavaPairDStream<String,String> dStream = buildKafkaDStream(jssc, docTopic, broker, consumerGroup);
		
//		JavaPairRDD<String,String> compute = dStream.compute(new Time(intervalMs));
		JavaRDD<String> dataRDD = jssc.sparkContext().emptyRDD();
		dataRDD.cache();
		
		final List<JavaRDD<String>> batches = new ArrayList<>();
		batches.add(dataRDD);

		dStream.foreachRDD(new VoidFunction<JavaPairRDD<String, String>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaPairRDD<String, String> rdd) throws Exception {

				rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void call(Iterator<Tuple2<String, String>> recordIterator) throws Exception {
						
						List<String> batch = new ArrayList<>();
						while (recordIterator.hasNext()) {
							Tuple2<String, String> tuple = recordIterator.next();
							String docName = tuple._1();
							if(null != docName){
								publishDocName(docName, broker, metaTopic);								
							}
							
							String docContent = tuple._2();
							if (null != docContent && !docContent.trim().isEmpty()) {
								batch.add(docContent);								
							}
						}
						// append batch to corpus
						if (batch.size() > 0) {
							JavaRDD<String> batchRDD = jssc.sparkContext().parallelize(batch);
							batchRDD.cache();
							
							batches.add(batchRDD);
							dispatchLDA(jssc, batches);							
						}
					}
				});
			}
		});

		// start the consumer
		jssc.start();

		// stay in infinite loop until terminated
		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
		}
	}

}
