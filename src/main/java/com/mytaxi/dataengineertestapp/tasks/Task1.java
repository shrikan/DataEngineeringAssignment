package com.mytaxi.dataengineertestapp.tasks;

import java.util.Properties;

import org.apache.spark.sql.SQLContext;

import com.mytaxi.dataengineertestapp.postgres.IDataLoader;
import com.mytaxi.dataengineertestapp.postgresimpl.DataLoader;

public class Task1 {

	public boolean task(SQLContext sqlContext, IDataLoader dataLoader, Properties config) {
		if (config == null)
			return false;

		String connectionUrl = config.getProperty("connectionUrl");
		String[] tables = config.getProperty("tables").split(",");

		// Load all the tables from csv to postgres
		try {
			for (String table : tables) {
				String tableName = config.getProperty(table + ".tableName");
				String filePath = config.getProperty(table + ".filePath");

				dataLoader = new DataLoader();
				dataLoader.toDb(sqlContext, connectionUrl, tableName, filePath);
			}
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

}
