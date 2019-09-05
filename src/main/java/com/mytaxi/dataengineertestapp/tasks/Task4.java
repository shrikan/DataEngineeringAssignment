package com.mytaxi.dataengineertestapp.tasks;

import static org.apache.spark.sql.functions.*;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.expressions.WindowSpec;

public class Task4 {

	public Dataset<Row> task(SQLContext sqlContext, Properties config) {
		if (config == null)
			return null;

		String connectionUrl = config.getProperty("connectionUrl");
//		String driverTable = config.getProperty("driver.tableName");
		String bookingTable = config.getProperty("booking.tableName");
//		String passengerTable = config.getProperty("passenger.tableName");

		try {
			// Load table into DataSets
			// Can use this table to calculate new driver additions for the year. Since no
			// drivers addition for 2015 or 2016 from the given data, Haven't used it.
//			Dataset<Row> driverDataset = sqlContext.read().format("jdbc").option("url", connectionUrl)
//					.option("dbtable", driverTable).option("user", "skanchi").load();

			//// Can use this table to calculate new passengers additions for the year.
			//// Since no
			// drivers addition for 2015 or 2016 from the given data, Haven't used it.
//			Dataset<Row> passengerDataset = sqlContext.read().format("jdbc").option("url", connectionUrl)
//					.option("dbtable", passengerTable).option("user", "skanchi").load();

			String filteredQueryfor2016 = "(select * from " + bookingTable
					+ " where extract(year from start_date)=2016) as t";
			Dataset<Row> bookingDataset = sqlContext.read().format("jdbc").option("url", connectionUrl)
					.option("dbtable", filteredQueryfor2016).option("user", "skanchi").load();

			Dataset<Row> filteredDataset = bookingDataset.repartition(bookingDataset.col("id_driver"), bookingDataset.col("start_date"));
			
			/*
			 * yearly figures for bookings, the average driver evaulation revenue for 2016.
			 */

			// Creating a kpi dataframe with first entry as total number of bookings
			Dataset<Row> kpiDataset = filteredDataset.select(count(filteredDataset.col("id")).alias("Total bookings"),
					sum(filteredDataset.col("tour_value")).alias("Revenue"),
					bround(max(filteredDataset.col("rating")), 3).alias("highest_driver_rating"),
					bround(min(filteredDataset.col("rating")), 3).alias("least_driver_rating"),
					bround(avg(filteredDataset.col("rating")), 3).alias("avg_driver_rating"));

			Dataset<Row> revenueDataset = filteredDataset.groupBy(year(filteredDataset.col("start_date")).alias("year"))
					.agg(sum("tour_value").alias("revenue"));
			WindowSpec window = org.apache.spark.sql.expressions.Window.orderBy("year");

			Dataset<Row> lagCreatedDatset = revenueDataset.withColumn("prev_value", lag("revenue", 1).over(window));
			Dataset<Row> yoyRevenueGrowth = lagCreatedDatset.selectExpr("revenue-prev_value as yoy_growth_in_revenue",
					"(revenue-prev_value)/prev_value*100 as percentage_growth").where("year=2016");

			Dataset<Row> finalDataset = kpiDataset.join(yoyRevenueGrowth);
			Dataset<Row> avgRevenueFromDriver = filteredDataset.groupBy("id_driver")
					.agg(sum("tour_value").alias("driver_earnings"))
					.agg(bround(avg("driver_earnings"), 3).alias("avg_driver_earning"));

			Dataset<Row> kpiReportDataset = finalDataset.join(avgRevenueFromDriver);
			return kpiReportDataset;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
}
