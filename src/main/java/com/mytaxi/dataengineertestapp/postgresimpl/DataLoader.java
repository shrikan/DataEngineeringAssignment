package com.mytaxi.dataengineertestapp.postgresimpl;

import java.util.Properties;

import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.StructType;

import com.mytaxi.dataengineertestapp.postgres.IDataLoader;
import com.mytaxi.dataengineertestapp.utils.Schemas;

public class DataLoader implements IDataLoader {
	StructType schema = null;
	static Properties properties = null;

	static {
		properties = new Properties();
		properties.put("user", "skanchi");
		properties.put("driver", "org.postgresql.Driver");
	}

	@Override
	public void toDb(SQLContext sqlContext, String connectionUrl, String tableName, String filePath) {

		Schemas schemaLookup = new Schemas();
		schema = schemaLookup.getSchema(tableName);

		Dataset<Row> dataSet = sqlContext.read().format("com.databricks.spark.csv").schema(schema)
				.option("timestampFormat", "yyyy-MM-dd HH:mm").load(filePath);

		DataFrameWriter<Row> dfWriter = dataSet.write().mode(SaveMode.Append);
		dfWriter.jdbc(connectionUrl, tableName, properties);
	}
}
