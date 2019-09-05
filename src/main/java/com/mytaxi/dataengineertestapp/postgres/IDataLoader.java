package com.mytaxi.dataengineertestapp.postgres;

import org.apache.spark.sql.SQLContext;

public interface IDataLoader
{
    /**
     * Load data with any format to any Database's type.
     *
     * @param sqlContext     :
     * @param connectionUrl: connection url to database
     * @param tableName      : table name in db.
     * @param filePath       : file to upload
     */
    void toDb(SQLContext sqlContext, String connectionUrl, String tableName, String filePath);
}
