package com.delta.demo.main;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

public class ACID {

    public static StructType packageRawDataSchema() {
        return new StructType(
                new StructField[] { new StructField("id", DataTypes.LongType, false, Metadata.empty())});
    }

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession.Builder sparkSessionBuilder = SparkSession.builder();
        sparkSessionBuilder.appName("Spark Delta Demo");
        if(args.length > 0 && args[0].equalsIgnoreCase("local"))
            sparkSessionBuilder.master("local[4]");

        SparkSession spark = sparkSessionBuilder.getOrCreate();

        saveData(spark);

        appendData_Error(spark);

        showData(spark);

        spark.stop();
    }

    private static void saveData(SparkSession spark) {
        spark.range(0, 10).coalesce(1).write().format("delta").save("src/main/resources/delta/delta-table15");
    }

    private static void appendData_Error(SparkSession spark) {
        Dataset<Long> primitiveDS = spark.range(10, 20);
        Encoder<Long> longEncoder = Encoders.LONG();
        Dataset<Long> primitiveDS1 = primitiveDS.repartition(1).map((MapFunction<Long, Long>) id -> {
            if (id > 15)
                throw new RuntimeException("Write Error");
            return id;
        }, longEncoder);

        Dataset<Row> finalDS = primitiveDS1.withColumnRenamed("value", "id");

        finalDS.write().mode("append").format("delta").save("src/main/resources/delta/delta-table15");
    }

    private static void showData(SparkSession spark) {
        Dataset<Row> df = spark.read().format("delta").load("src/main/resources/delta/delta-table15");
        df.show();
    }

}
