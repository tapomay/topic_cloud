package cs185c.project;


import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka09.ConsumerStrategies;
import org.apache.spark.streaming.kafka09.KafkaUtils;
import org.apache.spark.streaming.kafka09.LocationStrategies;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import scala.Tuple2;

/**
 *
 */

public final class TCloudSparkApp {
	private static final ObjectMapper objectMapper = new ObjectMapper();

	public static void main(String[] args) {
		if (args.length < 6) {
			System.err.println("Usage: StockSparkApp <broker> <master> <in-topic> <out-topic> <cg> <interval>");
			System.err.println("eg: StockSparkApp localhost:9092 localhost:2181 test out mycg 5000");
			System.exit(1);
		}

		// set variables from command-line arguments
		final String broker = args[0];
		String master = args[1];
		String inTopic = args[2];
		final String outTopic = args[3];
		String consumerGroup = args[4];
		long interval = Long.parseLong(args[5]);

		// define topic to subscribe to
		final Pattern topicPattern = Pattern.compile(inTopic, Pattern.CASE_INSENSITIVE);

		// set Kafka client parameters
		Map<String, Object> kafkaParams = new HashMap<String, Object>();
		kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		kafkaParams.put("value.deserializer", "org.apache.kafka.connect.json.JsonDeserializer");
		kafkaParams.put("bootstrap.servers", broker);
		kafkaParams.put("group.id", consumerGroup);
		kafkaParams.put("enable.auto.commit", true);
		// kafkaParams.put("group.id", UUID.randomUUID().toString());

		// initialize the streaming context
		JavaStreamingContext jssc = new JavaStreamingContext(master, "StockSparkApp", new Duration(interval));
		Logger.getRootLogger().setLevel(Level.ERROR);
		// pull ConsumerRecords out of the stream
		final JavaInputDStream<ConsumerRecord<String, JsonNode>> messages = KafkaUtils.createDirectStream(jssc,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, JsonNode>SubscribePattern(topicPattern, kafkaParams));

		// pull values out of ConsumerRecords
		JavaPairDStream<String, JsonNode> keyValuePairs = messages
				.mapToPair(new PairFunction<ConsumerRecord<String, JsonNode>, String, JsonNode>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, JsonNode> call(ConsumerRecord<String, JsonNode> record) throws Exception {
						System.out.println("Got message");
						String key = record.key();
						JsonNode val = record.value();
						Tuple2<String, JsonNode> ret = new Tuple2<String, JsonNode>(key, val);
						return ret;
					}
				});

		keyValuePairs.foreachRDD(new VoidFunction<JavaPairRDD<String, JsonNode>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaPairRDD<String, JsonNode> rdd) throws Exception {
				final long count = rdd.count();
				rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, JsonNode>>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void call(Iterator<Tuple2<String, JsonNode>> recordIterator) throws Exception {
						double sumHigh = 0, sumLow = 0, sumOpen = 0, sumClose = 0, lastClose = 0;
						long sumVolume = 0;
						String stockSymbol = null, lastTimestamp = null;
						Tuple2<String, JsonNode> tuple;
						int recordCount = 0;
						while (recordIterator.hasNext()) {
							// TODO get next record
							tuple = recordIterator.next();
							JsonNode jsonNode = tuple._2;
							PriceDataPoint priceDataPoint = objectMapper.convertValue(jsonNode, PriceDataPoint.class);
							System.out.println("R2D2: " + priceDataPoint);
							// TODO pull out timestamp, stockSymbol from record
							String timestamp = priceDataPoint.getTimestamp();
							stockSymbol = priceDataPoint.getStockSymbol();

							// TODO pull out metrics from record
							// TODO calculate sums (sumHigh += ... , sumLow +=
							// ..., etc)
							sumHigh = sumHigh + priceDataPoint.getHigh();
							sumLow = sumLow + priceDataPoint.getLow();
							sumOpen = sumOpen + priceDataPoint.getOpen();
							sumClose = sumClose + priceDataPoint.getClose();
							sumVolume = sumVolume + priceDataPoint.getVolume();

							lastClose = priceDataPoint.getClose();
							lastTimestamp = priceDataPoint.getTimestamp();
							recordCount += 1;
						}
						System.out.println("RecordCount: " + recordCount);

						if (recordCount > 0) {

							// TODO calculate meanHigh, meanLow, ...
							double meanHigh = sumHigh / recordCount;
							double meanLow = sumLow / recordCount;
							double meanOpen = sumOpen / recordCount;
							double meanClose = sumClose / recordCount;
							double meanVolume = sumVolume / recordCount;

							// configure Kafka producer props
							Properties producerProps = new Properties();
							producerProps.put("bootstrap.servers", broker);
							producerProps.put("key.serializer",
									"org.apache.kafka.common.serialization.StringSerializer");
							producerProps.put("value.serializer", "org.apache.kafka.connect.json.JsonSerializer");

							// TODO create new ObjectNode to put data in
							// TODO put key-value pairs in ObjectNode
							StatDataPoint statDataPoint = new StatDataPoint(stockSymbol, lastTimestamp, meanHigh,
									meanLow, meanOpen, meanClose, meanVolume, lastClose);
							ObjectNode value = objectMapper.valueToTree(statDataPoint);

							// TODO create a properly-parameterized
							// ProducerRecord
							ProducerRecord<String, JsonNode> rec = new ProducerRecord<String, JsonNode>(outTopic,
									value);

							// TODO instantiate the KafkaProducer
							KafkaProducer<String, JsonNode> producer = new KafkaProducer<String, JsonNode>(
									producerProps);
							// TODO send the producer record
							producer.send(rec);
							System.out.println("Sent message  " + rec);

							// TODO close the producer
							producer.close();
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
