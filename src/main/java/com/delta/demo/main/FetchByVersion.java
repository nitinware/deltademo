package com.delta.demo.main;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class FetchByVersion {


    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession.Builder sparkSessionBuilder = SparkSession.builder();
        sparkSessionBuilder.appName("Spark Delta Demo");
        if(args.length > 0 && args[0].equalsIgnoreCase("local"))
            sparkSessionBuilder.master("local[4]");

        SparkSession spark = sparkSessionBuilder.getOrCreate();

        Dataset<Row> itemDF = spark.read().format("delta").option("versionAsOf", 1).load("src/main/resources/delta/delta-table20");

        itemDF.show();

        spark.stop();
    }

}
