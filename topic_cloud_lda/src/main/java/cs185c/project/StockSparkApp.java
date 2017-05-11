package cs185c.project;



import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.clustering.DistributedLDAModel;
import org.apache.spark.mllib.clustering.LDA;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka09.ConsumerStrategies;
import org.apache.spark.streaming.kafka09.KafkaUtils;
import org.apache.spark.streaming.kafka09.LocationStrategies;

import scala.Tuple2;




/**
 * @author jcasaletto
 * 
 * Consumes messages from input Kafka topic, calculates averages, then outputs averages to output Kafka topic
 *
 * Usage: StockSparkApp <broker> <master> <in-topic> <out-topic> <cg>
 *   <broker> is one of the servers in the kafka cluster
 *   <master> is either local[n] or yarn-client
 *   <in-topic> is the kafka topic to consume from
 *   <out-topic> is the kafka topic to produce to
 *   <cg> is the consumer group name
 *   <interval> is the number of milliseconds per batch
 *
 */

public final class StockSparkApp {
    public static void main(String[] args) {
        if (args.length < 6) {
            System.err.println("Usage: StockSparkApp <broker> <master> <in-topic> <out-topic> <cg> <interval>");
            System.err.println("eg: StockSparkApp localhost:9092 localhost:2181 test out mycg 5000");
            System.exit(1);
        }
        
        final SparkConf conf = new SparkConf().setAppName("LDA Example");
        final JavaSparkContext sc = new JavaSparkContext(conf);

        // set variables from command-line arguments
        final String broker = args[0];
        String master = args[1];
        String inTopic = args[2];
        final String outTopic = args[3];
        String consumerGroup = args[4];
        long interval = Long.parseLong(args[5]);
        
        final CircularFifoQueue<String> cfq = new CircularFifoQueue(1000);
        
        // define topic to subscribe to
        final Pattern topicPattern = Pattern.compile(inTopic, Pattern.CASE_INSENSITIVE);
    
        // set Kafka client parameters
        Map<String, Object> kafkaParams = new HashMap<String, Object>();
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.connect.json.JsonDeserializer");
        kafkaParams.put("bootstrap.servers", broker);
        kafkaParams.put("group.id", consumerGroup);
        kafkaParams.put("enable.auto.commit", true);

        // initialize the streaming context
        JavaStreamingContext jssc = new JavaStreamingContext(master, "StockSparkApp", new Duration(interval));

        // pull ConsumerRecords out of the stream
        final JavaInputDStream<ConsumerRecord<String, String>> messages = 
                        KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>SubscribePattern(topicPattern, kafkaParams)
                      );
    
        // pull values out of ConsumerRecords 
        JavaPairDStream<String, String> keyValuePairs = messages.mapToPair(new PairFunction<ConsumerRecord<String, String>, String, String>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Tuple2<String, String> call(ConsumerRecord<String, String> record) throws Exception {
                // TODO replace 'null' with key-value pair as tuple2
                return new Tuple2(record.key(), record.value());
            }
        }); 
        

        
        keyValuePairs.foreachRDD(new VoidFunction<JavaPairRDD<String,String>>() {
            private static final long serialVersionUID = 1L;
            @Override
            public void call(JavaPairRDD<String, String> rdd) throws Exception {
                final long count = rdd.count();
                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public void call(Iterator<Tuple2<String, String>> recordIterator) throws Exception {
                    	String fileName="";
                    	String fileText = "";
                        Tuple2<String, String> tuple;
                        while(recordIterator.hasNext()) {
                            // TODO get next record
                            tuple = recordIterator.next();
                            // TODO pull out timestamp, stockSymbol from record
                            fileName = tuple._1();
                            fileText = tuple._2();
                            // TODO pull out metrics from record
                            String[] textArray = fileText.split("\\s");
                            
                            for(String eachWord : textArray)
                            {
                            	cfq.add(eachWord);
                            }
                            
                            String[] finalWords = cfq.toArray(textArray);
                            ArrayList<String> wordList = new ArrayList(cfq);
                            JavaRDD<String> data = sc.parallelize(wordList);
                            // TODO calculate sums (sumHigh += ... , sumLow += ..., etc)
                            
                            JavaRDD<Vector> parsedData = data.map(
                                    new Function<String, Vector>() {
                                      public Vector call(String s) {
                                        String[] sarray = s.trim().split(" ");
                                        double[] values = new double[sarray.length];
                                        for (int i = 0; i < sarray.length; i++) {
                                          values[i] = Double.parseDouble(sarray[i]);
                                        }
                                        return Vectors.dense(values);
                                      }
                                    }
                                );
                                // Index documents with unique IDs
                                JavaPairRDD<Long, Vector> corpus = JavaPairRDD.fromJavaRDD(parsedData.zipWithIndex().map(
                                    new Function<Tuple2<Vector, Long>, Tuple2<Long, Vector>>() {
                                      public Tuple2<Long, Vector> call(Tuple2<Vector, Long> doc_id) {
                                        return doc_id.swap();
                                      }
                                    }
                                ));
                                corpus.cache();
                                DistributedLDAModel ldaModel = (DistributedLDAModel)new LDA().setK(3).run(corpus);

                                // Output topics. Each is a distribution over words (matching word count vectors)
                                System.out.println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize()
                                    + " words):");
                                Matrix topics = ldaModel.topicsMatrix();
                                for (int topic = 0; topic < 3; topic++) {
                                  System.out.print("Topic " + topic + ":");
                                  for (int word = 0; word < ldaModel.vocabSize(); word++) {
                                    System.out.print(" " + topics.apply(word, topic));
                                  }
                                  System.out.println();
                                  sc.stop();
                                }
                                // Cluster the documents into three topics using LDA
                        }       
                        // configure Kafka producer props
                        Properties producerProps = new Properties();
                        producerProps.put("bootstrap.servers", broker);
                        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                        producerProps.put("value.serializer", "org.apache.kafka.connect.json.JsonSerializer");
                        
                        // TODO create new ObjectNode to put data in
                        
                        
                        /*ProducerRecord<String, JsonNode> record = new ProducerRecord<String, JsonNode>(outTopic, stockSymbol, value);
                        
                        Properties props = new Properties();
                        props.put("value.serializer", "org.apache.kafka.connect.json.JsonSerializer");
                        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                        props.put("bootstrap.servers", broker);
                        KafkaProducer<String, JsonNode> producer = new KafkaProducer<String, JsonNode>(props);
                        producer.send(record);*/
                        //producer.close();
                        
                    }                     
                });
            }           
        });
    
        // start the consumer
        jssc.start();
    
        // stay in infinite loop until terminated
        try {
            jssc.awaitTermination();
        } 
        catch (InterruptedException e) {
        }
    }
}

