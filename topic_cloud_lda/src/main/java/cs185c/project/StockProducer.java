package cs185c.project;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
        
public class StockProducer {
    // Set the stream and topic to publish to.
    public static String topic;
    
    static
    {
    	  ConsoleAppender console = new ConsoleAppender(); //create appender
    	  //configure the appender
    	  String PATTERN = "%d [%p|%c|%C{1}] %m%n";
    	  console.setLayout(new PatternLayout(PATTERN)); 
    	  console.setThreshold(Level.DEBUG);
    	  console.activateOptions();
    	  //add appender to any Logger (here is root)
    	  Logger.getRootLogger().addAppender(console);
    }
        
    // Declare a new producer
    public static KafkaProducer<String, String> producer;
        
    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException, TimeoutException {
        // check command-line args
//        if(args.length != 4) {
//            System.err.println("usage: StockProducer <broker-socket> <input-file> <output-topic> <sleep-time>");
//            System.err.println("eg: StockProducer localhost:9092 /user/user01/LAB2/urlList.txt docs 1000");
//            System.exit(1);
//        }
        
        // initialize variables
        String brokerSocket = "localhost:9092";
//        String inputFile = "doc_data/main.txt";
        String inputFile = "doc_data/nlm500.txt";
        String outputTopic = "docs";
        long sleepTime = Long.parseLong("1000");
        BufferedReader br;
        String line = "";
        String filePath = "";
        // configure the producer
        configureProducer(brokerSocket);
        
        // TODO create a buffered file reader for the input file
        br = new BufferedReader(new FileReader(inputFile));
        
        while((filePath = br.readLine())!= null)
        {
        	String fileContents = "";
        	BufferedReader brForEachFile = new BufferedReader(new FileReader(filePath));
        	
     
        	while((line = brForEachFile.readLine()) != null)
        	{
        		fileContents = fileContents + line + " ";
        	}
        	brForEachFile.close();	
        	ProducerRecord<String, String> record = new ProducerRecord<String, String>(outputTopic, filePath, fileContents);
        	producer.send(record);
        	Thread.sleep(sleepTime);
        }
        br.close();
        producer.close();
        
    }

    public static void configureProducer(String brokerSocket) {
        Properties props = new Properties();
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("bootstrap.servers", brokerSocket);
        producer = new KafkaProducer<String, String>(props);
    }
}
