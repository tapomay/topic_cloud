package cs185c.project;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.LDA;
import org.apache.spark.mllib.clustering.LDAModel;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import scala.Tuple2;

/**
 * Consolidating relevant examples from:
 * https://github.com/apache/spark/tree/master/examples/src/main/java/org/apache/spark/examples
 * 
 * @author tadey
 *
 */
public class TopicCloudSparkAppMod {
	private static final Pattern SPACE = Pattern.compile(" ");
	private static final int STOP_WORD_COUNT = 20;
	private static final int TOPIC_COUNT = 10;

	private static Integer currentFileIndex = 1;
	private static final ObjectMapper mapper = new ObjectMapper();
	private static final int TOKEN_LIMIT = 10;
	
	public static KafkaConsumer<String, String> configureConsumer(String brokerSocket, String groupId) {
		Properties props = new Properties();
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("bootstrap.servers", brokerSocket);
		props.put("auto.commit.enable", true);
		props.put("group.id", UUID.randomUUID().toString());
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		return consumer;
	}

	public static KafkaProducer<String, JsonNode> configureProducer(String brokerSocket) {
        Properties props = new Properties();
        props.put("value.serializer", "org.apache.kafka.connect.json.JsonSerializer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("bootstrap.servers", brokerSocket);
        KafkaProducer<String, JsonNode> producer = new KafkaProducer<String, JsonNode>(props);
        return producer;
    }

	public static void main(String[] args) throws Exception {

		String brokerSocket = "localhost:9092";
		String inputTopic = "docs";
		String groupId = "mycg";
		long pollTimeOut = 1000l;
        String outputTopic = "tcdata";

		KafkaConsumer<String, String> kafkaConsumer = configureConsumer(brokerSocket, groupId);
		kafkaConsumer.subscribe(Arrays.asList(inputTopic));
        long sleepTime = 1000l;
        
        
        // configure the producer
        KafkaProducer<String,JsonNode> kafkaProducer = configureProducer(brokerSocket);
        
		while (true) {
			ConsumerRecords<String, String> records = kafkaConsumer.poll(pollTimeOut);
			int recordCount = 0;
			List<String> contentArr = new ArrayList<>();
			for (ConsumerRecord<String, String> consumerRecord : records) {
				String content = consumerRecord.value();
				contentArr.add(content);
				recordCount += 1;
			}
			if (recordCount > 0) {
				for (String content : contentArr) {
					writeToFileIndex(currentFileIndex++, content);
				}
				Map<String, Double> scores = testLDAStr("data/*.txt");
				publish(scores, kafkaProducer, outputTopic);
			}
		}
		
		// TODO Auto-generated method stub
//		TopicCloudSparkAppMod app = new TopicCloudSparkAppMod();
		// app.wordCount("problem.txt");
		// app.testLDANum("sample_lda_data.txt");
		// app.testLDAStr("problem.txt");
	}

	private static void publish(Map<String, Double> scores, KafkaProducer<String,JsonNode> kafkaProducer, String outputTopic) {
		ObjectNode objectNode = mapper.createObjectNode();
		Set<Entry<String,Double>> entrySet = scores.entrySet();
		for (Entry<String, Double> entry : entrySet) {
			String token = entry.getKey();
			Double score = entry.getValue();
			objectNode.put(token, score);
		}
		ProducerRecord<String, JsonNode> rec = new ProducerRecord<String, JsonNode>(outputTopic, objectNode);
		kafkaProducer.send(rec);
		System.out.println("Sent message  " + rec);		
	}

	private static void writeToFileIndex(Integer index, String content) throws FileNotFoundException {
		String filename = String.format("data/%s-%d.txt", "datafile", index);
		try(  PrintWriter out = new PrintWriter( filename )  ){
		    out.println( content );
		}
		System.out.println("Created: " + filename);
	}


	public static Map<String, Double> testLDAStr(String filepath) throws Exception {
		/**
		 * https://gist.github.com/jkbradley/ab8ae22a8282b2c8ce33
		 * 
		 */
		SparkConf conf = new SparkConf().setAppName("JavaKLatentDirichletAllocationExample");
		conf.setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		// String path = "data/mllib/sample_lda_data.txt";
//		JavaRDD<String> data = jsc.textFile(filepath);
		JavaRDD<String> data = jsc.wholeTextFiles(filepath).values();
//		 jsc.wholeTextFiles(filepath).values(); //for directory

		JavaRDD<String> dataSplit = data
				.flatMap(filedata -> Arrays.asList(filedata.toLowerCase().split(" ")).iterator());

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

		// val termCounts: Array[(String, Long)] =
		// tokenized.flatMap(_.map(_ -> 1L)).reduceByKey(_ +
		// _).collect().sortBy(-_._2)
		JavaPairRDD<String, Integer> wordCounts = dataTokens.mapToPair(word -> new Tuple2<>(word, 1))
				.reduceByKey((a, b) -> a + b);
		List<Tuple2<String, Integer>> collect = wordCounts.collect();

		Comparator<Tuple2<String, Integer>> comp = new Comparator<Tuple2<String, Integer>>() {

			@Override
			public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
				return o2._2() - o1._2(); // descending
			}
		};

		// Collections.sort(collect, comp); //unsupported since immutable list
		List<Tuple2<String, Integer>> collectSorted = collect.stream().sorted(comp).collect(Collectors.toList());

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
		JavaPairRDD<String[], Long> zipWithIndex = dataFilesRDD.zipWithIndex();

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
				} else {
					System.out.println("Token not found in index: " + tokenIdx);
				}
			}
			List<Tuple2<Integer, Double>> tupleList = countsMap.entrySet().stream()
					.map(entry -> new Tuple2<Integer, Double>(entry.getKey(), entry.getValue()))
					.collect(Collectors.toList());
			Vector v = Vectors.sparse(vocabCount, tupleList);
			return new Tuple2<Long, Vector>(docId, v);
		});

		JavaPairRDD<Long, Vector> corpus = JavaPairRDD.fromJavaRDD(zipWithIndex.map(vectorizerFunc));

		LDAModel ldaModel = new LDA().setK(TOPIC_COUNT).run(corpus);

		Tuple2<int[], double[]>[] describeTopics = ldaModel.describeTopics(10);
		int topic = 0;

		Map<Integer, String> revVocabIndex = new HashMap<Integer, String>();
		vocabIndex.forEach(new BiConsumer<String, Integer>() {
			@Override
			public void accept(String t, Integer u) {
				revVocabIndex.put(u, t);
			}
		});


		Map<String, Double> tokenScores = new HashMap<>();
		for (Tuple2<int[], double[]> topicDescription : describeTopics) {
			System.out.print("\nTopic " + topic + ":");
			int[] tokenIndices = topicDescription._1();
			double[] tokenWeights = topicDescription._2();
			for (int i = 0; i < tokenWeights.length; i++) {
				System.out.print(String.format("\n%d: %s : %f", tokenIndices[i], revVocabIndex.get(tokenIndices[i]),
						tokenWeights[i]));
				tokenScores.put(revVocabIndex.get(tokenIndices[i]), tokenWeights[i]);
			}
			topic++;
		}
		Comparator<? super Entry<String, Double>> comparator = new Comparator<Entry<String, Double>>() {

			@Override
			public int compare(Entry<String, Double> o1, Entry<String, Double> o2) {
				return o2.getValue().intValue() - o1.getValue().intValue();
			}
		};
		
		List<Entry<String, Double>> sorted = tokenScores.entrySet().stream().sorted(comparator).collect(Collectors.toList());
		Map<String, Double> ret = new HashMap<>();
		for (int i = 0; i < TOKEN_LIMIT && i < sorted.size(); i++) {
			Entry<String, Double> entry = sorted.get(i);
			ret.put(entry.getKey(), entry.getValue());
		}
		// Output topics. Each is a distribution over words (matching word count
		// vectors)

		jsc.close();
		return ret;
	}
}
