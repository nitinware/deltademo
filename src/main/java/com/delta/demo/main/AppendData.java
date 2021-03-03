package com.delta.demo.main;

import com.delta.demo.model.Item;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;

import java.util.ArrayList;
import java.util.List;

public class AppendData {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        SparkSession.Builder sparkSessionBuilder = SparkSession.builder();
        sparkSessionBuilder.appName("Spark Delta Demo");
        if(args.length > 0 && args[0].equalsIgnoreCase("local"))
            sparkSessionBuilder.master("local[4]");

        SparkSession spark = sparkSessionBuilder.getOrCreate();

        appendDelta(spark);

        showDelta(spark);

        spark.stop();
    }

    private static void appendDelta(SparkSession spark) {
        List<Item> itemList = new ArrayList<>();

        Item item1 = new Item();
        item1.setItemId(204844661L);
        item1.setDescShort("Circuit Breaker");
        item1.setDescLong("Homeline 20 Amp Single-Pole Plug-On Neutral Dual Function (CAFCI and GFCI) Circuit Breaker");
        itemList.add(item1);

        Item item2 = new Item();
        item2.setItemId(100161118L);
        item2.setDescShort("Microwave Fuses");
        item2.setDescLong("ABC Series 20 Amp Fast-Act Microwave Fuses");
        itemList.add(item2);

        // Encoders convert JVM object of type T to and from the internal SQL representation
        Encoder<Item> itemEncoder = Encoders.bean(Item.class);
        Dataset<Row> itemDF = spark.createDataset(itemList, itemEncoder).toDF();

        // Write Spark Dataframe to delta format.
        itemDF.write().format("delta").mode("append").save("src/main/resources/delta/delta-table20");
    }

    private static void showDelta(SparkSession spark) {
        // Read Spark Dataframe into delta format.
        Dataset<Row> itemDF = spark.read().format("delta").load("src/main/resources/delta/delta-table20");
        itemDF.show();
    }

}
