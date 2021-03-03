package com.delta.demo.main;
import com.delta.demo.model.Item;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;

import java.util.ArrayList;
import java.util.List;

public class DeltaMain {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        SparkSession.Builder sparkSessionBuilder = SparkSession.builder();
        sparkSessionBuilder.appName("Spark Delta Demo");
        if(args.length > 0 && args[0].equalsIgnoreCase("local"))
            sparkSessionBuilder.master("local[4]");

        SparkSession spark = sparkSessionBuilder.getOrCreate();

        saveAsDelta(spark);

        showDelta(spark);

        spark.stop();
    }

    private static void saveAsDelta(SparkSession spark) {

        List<Item> itemList = new ArrayList<>();

        Item item1 = new Item();
        item1.setItemId(202386562L);
        item1.setDescShort("Shower Filter");
        item1.setDescLong("Universal Shower Filter in Chrome");
        itemList.add(item1);

        Item item2 = new Item();
        item2.setItemId(301579504L);
        item2.setDescShort("Charcoal Grill");
        item2.setDescLong("Signature Series Table Top Charcoal Grill");
        itemList.add(item2);

        // Encoders convert JVM object of type T to and from the internal SQL representation
        Encoder<Item> itemEncoder = Encoders.bean(Item.class);
        Dataset<Row> itemDF = spark.createDataset(itemList, itemEncoder).toDF();

        // Write Spark Dataframe to delta format.
        itemDF.write().format("delta").save("src/main/resources/delta/delta-table20");

    }

    private static void showDelta(SparkSession spark) {
        // Read Spark Dataframe into delta format.
        Dataset<Row> itemDF = spark.read().format("delta").load("src/main/resources/delta/delta-table20");
        itemDF.show();
    }

}
