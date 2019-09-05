package com.mytaxi.dataengineertestapp.tasks;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.sum;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class Task2 {
	public Dataset<Row> task(SQLContext sqlContext, Properties config) {
		if (config == null)
			return null;

		String connectionUrl = config.getProperty("connectionUrl");
		String driverTable = config.getProperty("driver.tableName");
		String bookingTable = config.getProperty("booking.tableName");

		try {
			// Load table into DataSets
			Dataset<Row> driverDataset = sqlContext.read().format("jdbc").option("url", connectionUrl)
					.option("dbtable", driverTable).option("user", "skanchi").load();

			String filteredQueryfor2016 = "(select * from " + bookingTable + " where extract(year from start_date)=2016) as t";
			Dataset<Row> bookingDataset = sqlContext.read().format("jdbc").option("url", connectionUrl)
					.option("dbtable", filteredQueryfor2016).option("user", "skanchi").load();

			Dataset<Row> filteredDataset = bookingDataset.repartition(bookingDataset.col("id_driver"));

			// Calculating total tour_value and average of rating post group by clause
			Dataset<Row> calculatedDataset = filteredDataset.groupBy(filteredDataset.col("id_driver")).agg(
					sum(filteredDataset.col("tour_value")).alias("total_tour_value"),
					avg(filteredDataset.col("rating")).alias("avg_rating"));

			Dataset<Row> joinedDataset = calculatedDataset.join(driverDataset,
					driverDataset.col("id").equalTo(calculatedDataset.col("id_driver")), "inner");

			Dataset<Row> finalDataSet = joinedDataset.select("id_driver", "name", "avg_rating", "total_tour_value");

			// Sort based on tour_value and avg_rating, with primary sort column as
			// tour_value and pick first 10 rows
			Dataset<Row> topTenDrivers = finalDataSet
					.sort(finalDataSet.col("total_tour_value").desc(), finalDataSet.col("avg_rating").desc()).limit(10);


			return topTenDrivers;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
}
