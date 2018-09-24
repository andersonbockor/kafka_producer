package kafka_producer.kafka_producer;

import java.io.UnsupportedEncodingException;
import java.sql.Timestamp;
import java.util.Base64;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.admin.RackAwareMode.Safe$;	
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;

public class Producer {
	public static void main(String[] args) throws InterruptedException, UnsupportedEncodingException {

		Properties properties = new Properties();
		properties.put("bootstrap.servers", "localhost:9092");
		properties.put("acks", "0");
		properties.put("retries", "1");
		properties.put("batch.size", "20971520");
		properties.put("linger.ms", "33");
		properties.put("max.request.size", "2097152");
		properties.put("compression.type", "gzip");
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		createTopic("Bomba1");
		createTopic("Bomba2");
		
		runMainLoop(args, properties);
	}

	private static void createTopic(String name) throws InterruptedException {
		
		//create topic
		ZkClient zkClient = new ZkClient("localhost:2181", 10000, 8000, ZKStringSerializer$.MODULE$);		
		ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection("localhost:2181"), false);
				
		if (!AdminUtils.topicExists(zkUtils, name)) {
			tryCreate(zkUtils, name, 3);
	    } else {
	      System.out.println("Topic \"" + name + "\" already exists! Nothing to do here.");
	    }

	zkClient.close();
		
	}
	
	private static void tryCreate(ZkUtils zkUtils, String topicName, int nRetriesLeft) throws InterruptedException {
	    System.out.println("Creating topic " + topicName);
	    Properties topicConfig = new Properties(); // add per-topic configurations settings here
	    try {
	      int partitions = 1;
	      int replication = 0;
	      RackAwareMode rackAwareMode = Safe$.MODULE$;
	      AdminUtils.createTopic(zkUtils, topicName, partitions, replication, topicConfig, rackAwareMode);
	    } catch (Exception e) {
	      System.err.println("Topic create failed due to " + e.toString());
	      if (nRetriesLeft <= 0) {
	        throw new RuntimeException("Failed to create topic \"" + topicName + "\". Is Kafka and Zookeeper running?");
	      } else {
	        System.out.println("Failed to create topic, trying again in 5 seconds...");
	        TimeUnit.SECONDS.sleep(5);
	        tryCreate(zkUtils, topicName, nRetriesLeft - 1);
	      }
	    }

	    System.out.println("Successfully created topic '" + topicName + "'");
	}

	static void runMainLoop(String[] args, Properties properties)
			throws InterruptedException, UnsupportedEncodingException {

		// Create Kafka producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

		try {
			while (true) {
				Thread.sleep(1000);
				String id = "device-" + getRandomNumberInRange(1, 5);
				producer.send(new ProducerRecord<String, String>("Bomba1", id, getMsg("Bomba1"+id)));
				producer.send(new ProducerRecord<String, String>("Bomba2", id, getMsg("Bomba2"+id)));
			}
		} finally {
			producer.close();
		}
	}

	public static String getMsg(String id) throws UnsupportedEncodingException {

		Gson gson = new Gson();

		String timestamp = new Timestamp(System.currentTimeMillis()).toString();

		JsonObject obj = new JsonObject();
		obj.addProperty("id", id);
		obj.addProperty("timestamp", timestamp);
		obj.addProperty("data", Base64.getEncoder().encodeToString("this is my message data ...".getBytes("utf-8")));
		String json = gson.toJson(obj);

		return json;

	}

	private static int getRandomNumberInRange(int min, int max) {

		Random r = new Random();
		return r.ints(min, (max + 1)).findFirst().getAsInt();

	}

}
