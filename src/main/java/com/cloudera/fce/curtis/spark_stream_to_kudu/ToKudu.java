package com.cloudera.fce.curtis.spark_stream_to_kudu;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.Duration;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.*;

public class ToKudu {
  public static void main(String[] args) throws Exception {
    final String inputHostName = args[0];
    final String inputPortName = args[1];
    final String kuduMasters = args[2];
    final String kuduTableName = args[3];

    SparkConf sparkConf = new SparkConf().setAppName("ToKudu");
    final SparkSession spark  = SparkSession.builder().config(sparkConf).getOrCreate();
    JavaStreamingContext jssc  = new JavaStreamingContext(
        new JavaSparkContext(spark.sparkContext()), new Duration(5000));
    JavaReceiverInputDStream<String> stream = jssc.socketTextStream(
        inputHostName, Integer.parseInt(inputPortName));

    stream.foreachRDD(new VoidFunction<JavaRDD<String>>() {
      @Override
      public void call(JavaRDD<String> rdd) {
        JavaRDD<Row> rowRDD = rdd.map(new Function<String, Row>() {
          @Override
          public Row call(String rec) {
            String[] flds    = rec.split(",");
            Long measure     = Long.parseLong(flds[0]);
            Integer vehicles = Integer.parseInt(flds[1].trim());
            return RowFactory.create(measure, vehicles);
          }
        });

        System.out.println("***** Mohit got " + rowRDD.count());

        StructType schema = DataTypes.createStructType(new StructField[] {
            DataTypes.createStructField("measurement_time", DataTypes.LongType, false),
            DataTypes.createStructField("number_of_vehicles", DataTypes.IntegerType, true)
        });

        Dataset<Row> dataFrame = spark.createDataFrame(rowRDD, schema);
        dataFrame.registerTempTable("traffic");

        String query = "SELECT UNIX_TIMESTAMP() * 1000 as_of_time,"           +
            "       ROUND(AVG(number_of_vehicles),2) avg_num_veh,"  +
            "       MIN(number_of_vehicles) min_num_veh,"           +
            "       MAX(number_of_vehicles) max_num_veh,"           +
            "       MIN(measurement_time) first_meas_time,"         +
            "       MAX(measurement_time) last_meas_time "          +
            "   FROM traffic";
        Dataset resultsDataFrame = spark.sql(query);
        final Map<String, String> kuduOptions = new HashMap<>();
        kuduOptions.put("kudu.table",  kuduTableName);
        kuduOptions.put("kudu.master", kuduMasters);
        kuduOptions.put("kudu.table", "impala::default." + kuduTableName);
        resultsDataFrame.write().format("org.apache.kudu.spark.kudu").options(kuduOptions).mode("append").save();
      }
    });

    jssc.start();
    jssc.awaitTermination();
  }
}