package com.delta.demo.main;

import com.delta.demo.model.Item;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;

import java.util.ArrayList;
import java.util.List;

public class SchemaChange {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession.Builder sparkSessionBuilder = SparkSession.builder();
        sparkSessionBuilder.appName("Spark Delta Demo");
        if(args.length > 0 && args[0].equalsIgnoreCase("local"))
            sparkSessionBuilder.master("local[4]");

        SparkSession spark = sparkSessionBuilder.getOrCreate();

        add_items_with_updated_schem(spark);

        showDelta(spark);

        spark.stop();
    }

    private static void add_items_with_updated_schem(SparkSession spark) {
        List<Item> itemList = new ArrayList<>();

        Item item1 = new Item();
        item1.setItemId(202300503L);
        item1.setDescShort("PVC Pipe");
        item1.setDescLong("1-1/2 in. x 24 in. PVC Sch. 40 DWV Pipe");
        //item1.setStore(8119); // new field /schema change
        itemList.add(item1);

        // Encoders convert JVM object of type T to and from the internal SQL representation
        Encoder<Item> itemEncoder = Encoders.bean(Item.class);
        Dataset<Row> itemDF = spark.createDataset(itemList, itemEncoder).toDF();

        // Append to Spark Dataframe an item with updated schema in delta format.
        itemDF.write().format("delta").mode("append").save("src/main/resources/delta/delta-table20");

        //itemDF.write().format("delta").mode("append").option("mergeSchema", "true").save("src/main/resources/delta/delta-table20");

    }

    private static void showDelta(SparkSession spark) {
        // Read Spark Dataframe into delta format.
        Dataset<Row> itemDF = spark.read().format("delta").load("src/main/resources/delta/delta-table20");
        itemDF.show();
    }

}
