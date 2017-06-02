package com.guilherme.kafka_samples;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author ghott
 *
 */
public class SimpleKafkaConsumer {

	private static final Logger LOG = LoggerFactory.getLogger(SimpleKafkaConsumer.class);
	private static KafkaConsumer<String, String> consumer;

	public static void main(String[] args) throws Exception {
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

		String origin = "origin.";

		// Kafka consumer configuration settings
		List<String> topics = Arrays.asList(origin+"guilhermehott",origin+"Topic2",origin+"PlaidWebhook",origin+"at-least-once",origin+"exactly-once",origin+"exactly-once-test");

		Properties props = new Properties();
		props.put("bootstrap.servers", "dev-kafka.clout.com:9092");
		props.put("group.id", "test-consumer-group");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		consumer = new KafkaConsumer<String, String>(props);

		try {
			// Kafka Consumer subscribes list of topics here.
			consumer.subscribe(topics);

			// print the topic name
			LOG.info("Subscribed to topics {}", String.join(",", topics));

			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(100);
				for (ConsumerRecord<String, String> record : records){
					LOG.info("{} \t topic = {}, offset = {}, key = {}", dateFormat.format(new Date(record.timestamp())), record.topic(), record.offset(), record.key());
					LOG.info("message value = {}", record.value());
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			consumer.close();
		}
	}
}