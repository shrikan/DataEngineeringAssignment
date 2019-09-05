package com.mytaxi.dataengineertestapp.utils;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Schemas {
	private static StructType bookingSchema = null;
	private static StructType driverSchema = null;
	private static StructType passengerSchema = null;

	static {
		bookingSchema = DataTypes
				.createStructType(new StructField[] { DataTypes.createStructField("id", DataTypes.LongType, false),
						DataTypes.createStructField("date_created", DataTypes.TimestampType, false),
						DataTypes.createStructField("id_driver", DataTypes.LongType, false),
						DataTypes.createStructField("id_passenger", DataTypes.LongType, false),
						DataTypes.createStructField("rating", DataTypes.IntegerType, false),
						DataTypes.createStructField("start_date", DataTypes.TimestampType, false),
						DataTypes.createStructField("end_date", DataTypes.TimestampType, false),
						DataTypes.createStructField("tour_value", DataTypes.LongType, false) });
		driverSchema = DataTypes
				.createStructType(new StructField[] { DataTypes.createStructField("id", DataTypes.LongType, false),
						DataTypes.createStructField("date_created", DataTypes.TimestampType, false),
						DataTypes.createStructField("name", DataTypes.StringType, false) });
		passengerSchema = DataTypes
				.createStructType(new StructField[] { DataTypes.createStructField("id", DataTypes.LongType, false),
						DataTypes.createStructField("date_created", DataTypes.TimestampType, false),
						DataTypes.createStructField("name", DataTypes.StringType, false) });

	}
	public static StructType schema;

	public StructType getSchema(String table) {
		switch (table) {
		case "booking":
			return bookingSchema;
		case "driver":
			return driverSchema;
		case "passenger":
			return passengerSchema;
		default:
			return null;
		}
	}
}
