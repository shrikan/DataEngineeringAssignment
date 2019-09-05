package com.mytaxi.dataengineertestapp.kafkaImpl;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.mytaxi.dataengineertestapp.Application;
import com.mytaxi.dataengineertestapp.kafka.IProducer;

public class ProducerImpl implements IProducer {

	@Override
	public void send(String topic, Object payload) {
		Properties config = new Properties();
		try {
			config.load(Application.class.getClassLoader().getResourceAsStream("config.properties"));

		} catch (IOException e) {
			e.printStackTrace();
		}
		@SuppressWarnings("unchecked")
		Dataset<Row> topTenDriver = (Dataset<Row>) payload;
		List<String> msgPayload = topTenDriver.toJSON().collectAsList();
		
		KafkaProducerForReport producerThread = new KafkaProducerForReport(topic, msgPayload, config);
		producerThread.start();
	}

}
