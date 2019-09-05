package com.mytaxi.dataengineertestapp.tasks;

import static org.apache.spark.sql.functions.count;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class Task3 {

	public Dataset<Row> task(SQLContext sqlContext, Properties config) {
		if (config == null)
			return null;

		String connectionUrl = config.getProperty("connectionUrl");
		String driverTable = config.getProperty("driver.tableName");
		String bookingTable = config.getProperty("booking.tableName");
		String passengerTable = config.getProperty("passenger.tableName");

		try {
			// Load table into DataSets
			Dataset<Row> driverDataset = sqlContext.read().format("jdbc").option("url", connectionUrl)
					.option("dbtable", driverTable).option("user", "skanchi").load();

			Dataset<Row> bookingDataset = sqlContext.read().format("jdbc").option("url", connectionUrl)
					.option("dbtable", bookingTable).option("user", "skanchi").load();

			Dataset<Row> passengerDataset = sqlContext.read().format("jdbc").option("url", connectionUrl)
					.option("dbtable", passengerTable).option("user", "skanchi").load();


			Dataset<Row> filteredDataset = bookingDataset.repartition(bookingDataset.col("id_driver"), bookingDataset.col("id_passenger"));
			
			// Calculating total number of trips by grouping driver id and passenger id from
			// booking			
			Dataset<Row> calculatedDataset = filteredDataset
					.groupBy(bookingDataset.col("id_driver"), bookingDataset.col("id_passenger"))
					.agg(count(bookingDataset.col("id")).alias("total_trips"));

			Dataset<Row> driverJoinedDataset = calculatedDataset
					.join(driverDataset, driverDataset.col("id").equalTo(calculatedDataset.col("id_driver")), "inner")
					.withColumnRenamed("name", "driver_name").drop("id");

			Dataset<Row> joinedTable = driverJoinedDataset.join(passengerDataset,
					passengerDataset.col("id").equalTo(driverJoinedDataset.col("id_passenger")), "inner")
					.withColumnRenamed("name", "passenger_name");

			Dataset<Row> selectedColumnsDf = joinedTable.select("id_driver", "driver_name", "id_passenger",
					"passenger_name", "total_trips");

			Dataset<Row> topTenRelationDf = selectedColumnsDf.sort(selectedColumnsDf.col("total_trips").desc())
					.limit(10);

			return topTenRelationDf;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

}
