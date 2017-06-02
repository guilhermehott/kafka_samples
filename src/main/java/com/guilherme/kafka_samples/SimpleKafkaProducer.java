package com.guilherme.kafka_samples;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author ghott
 *
 */
public class SimpleKafkaProducer {
	
	private static final Logger LOG = LoggerFactory.getLogger(SimpleKafkaProducer.class);
	
	public static void main(String[] args) {
		
		String origin = "origin.";

		// Assign topicName to string variable
		String topicName = origin+"guilhermehott"; // args[0].toString();

		String message = "{"
				+ "\"accessToken\":\"access-sandbox-d6fd887f-cdab-4398-9dca-ec4653e1fb5f\","
				+ "\"startDate\":\"2017-03-01\","
				+ "\"endDate\":\"2017-04-01\""
				+ "}";

		// create instance for properties to access producer configs
		Properties props = new Properties();

		// Assign localhost id
		props.put("bootstrap.servers", "dev-kafka.clout.com:9092");

		// Set acknowledgements for producer requests.
		props.put("acks", "all");

		// If the request fails, the producer can automatically retry,
		props.put("retries", 0);

		// Specify buffer size in config
		props.put("batch.size", 16384);

		// Reduce the no of requests less than 0
		props.put("linger.ms", 1);

		// The buffer.memory controls the total amount of memory available to
		// the producer for buffering.
		props.put("buffer.memory", 33554432);

		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = null;

		try {
			producer = new KafkaProducer<String, String>(props);
			
			for (int i = 0; i < 2; i++) {
				producer.send(new ProducerRecord<String, String>(topicName, Integer.toString(i), message));
			}
			
			LOG.info("Message sent successfully to topic: {}", topicName);
			LOG.info("message: {}", message);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			producer.close();
		}
	}
}
