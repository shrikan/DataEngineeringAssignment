package com.mytaxi.dataengineertestapp;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import com.mytaxi.dataengineertestapp.kafka.IProducer;
import com.mytaxi.dataengineertestapp.kafkaImpl.ProducerImpl;
import com.mytaxi.dataengineertestapp.postgres.IDataLoader;
import com.mytaxi.dataengineertestapp.tasks.Task1;
import com.mytaxi.dataengineertestapp.tasks.Task2;
import com.mytaxi.dataengineertestapp.tasks.Task3;
import com.mytaxi.dataengineertestapp.tasks.Task4;

public class Application {
	static Logger logger = Logger.getLogger(Application.class.getName());

	public static void main(String[] args) {
		try {
			PrintStream out = new PrintStream(new FileOutputStream("log/log.out", true), true);
			System.setOut(out);
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}

		IDataLoader dataLoader = null;
		IProducer producer = new ProducerImpl();
		Properties config = new Properties();
		try {
			config.load(Application.class.getClassLoader().getResourceAsStream("config.properties"));
			logger.info("config file loaded successfully");
		} catch (IOException e) {
			logger.error("No config file found or error loading the same");
			System.exit(-1);
		}

		// Initialize spark and sql contexts.
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("MyTaxiTest");
		SparkContext sparkContext = new SparkContext(sparkConf);
		@SuppressWarnings("deprecation")
		SQLContext sqlContext = new SQLContext(sparkContext);

		logger.info("spark and sql context created successfully");

		Task1 task1 = new Task1();
		Task2 task2 = new Task2();
		Task3 task3 = new Task3();
		Task4 task4 = new Task4();

		// Task 01 : Load all csv files into postgres db
		logger.info("Running task 01: Load all csv files into postgres db");
		boolean isLoaded = task1.task(sqlContext, dataLoader, config);
		if (isLoaded) {
			logger.info("All the tables have been populated successfully");
		} else {
			logger.warn("Error while loading some of the tables, please check");
		}

		// Task 02: Create a report for Top 10 drivers from 2016
		Dataset<Row> topTenDriver = task2.task(sqlContext, config);
		System.out.println("TOP 10 DRIVERS OF THE YEAR 2016:");
		topTenDriver.show();

		// Task 03: TOP 10 strongest relationships between passengers and drivers
		Dataset<Row> topTenRelations = task3.task(sqlContext, config);
		System.out.println("TOP 10 DRIVER-PASSENGDER RELATIONS:");
		topTenRelations.show();

		// Task 04: KPI to know the overall performance of our company
		Dataset<Row> kpiReport = task4.task(sqlContext, config);
		System.out.println("KPI REPORT FOR THE YEAR 2016:");
		kpiReport.show();

		// Task 05: deliver above results to Management Reporting Tool // publish to
		// kafka
		// Task 02 output
		producer.send(config.getProperty("topic.task2"), topTenDriver);

		// Task 03 output
		producer.send(config.getProperty("topic.task3"), topTenRelations);

		// Task 04 output
		producer.send(config.getProperty("topic.task4"), kpiReport);
	}
}
