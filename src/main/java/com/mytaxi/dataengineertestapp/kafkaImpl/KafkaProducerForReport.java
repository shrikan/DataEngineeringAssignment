package com.mytaxi.dataengineertestapp.kafkaImpl;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaProducerForReport extends Thread {
	private final KafkaProducer<Integer, String> producer;
	private final String topic;
	private final List<String> messagePayload;

	private final String KAFKA_SERVER_URL;
	private final String CLIENT_ID;

	public KafkaProducerForReport(String topic, List<String> message, Properties config) {

		this.KAFKA_SERVER_URL = config.getProperty("kafka_server");
		this.CLIENT_ID = config.getProperty("client_id");

		Properties properties = new Properties();
		properties.put("bootstrap.servers", KAFKA_SERVER_URL);
		properties.put("client.id", CLIENT_ID);
		properties.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producer = new KafkaProducer<>(properties);
		this.topic = topic;
		this.messagePayload = message;
	}

	public void run() {
		for (String msg : messagePayload) {
			try {
				producer.send(new ProducerRecord<>(topic, msg)).get();
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
		}
		System.out.println("Pushed to Kafka Successfully");
		producer.close();
	}
}