package com.cloudera.fce.curtis.spark_stream_to_kudu;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;

import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValue;
import org.apache.kudu.spark.kudu.KuduContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;


public class SparkStreamingRunner {

  private static Logger LOG = LoggerFactory.getLogger(SparkStreamingRunner.class);
  public static final String APPLICATION_NAME_PROPERTY = "application.name";
  public static final String BATCH_MILLISECONDS_PROPERTY = "application.batch.milliseconds";
  public static final String WINDOW_MILLISECONDS_PROPERTY = "application.window.batch.milliseconds";
  public static final String NUM_EXECUTORS_PROPERTY = "application.executors";
  public static final String NUM_EXECUTOR_CORES_PROPERTY = "application.executor.cores";
  public static final String EXECUTOR_MEMORY_PROPERTY = "application.executor.memory";
  public static final String SPARK_CONF_PROPERTY_PREFIX = "application.spark.conf";
  public static final String SPARK_SESSION_ENABLE_HIVE_SUPPORT = "application.hive.enabled";

  private static final String KAFKA_GROUP_ID_CONFIG = "kafka.group.id";
  public static final String KAFKA_BROKERS_CONFIG = "kafka.brokers";
  public static final String KAFKA_TOPIC_CONFIG = "kafka.topics";
  public static final String KAFKA_ENCODING_CONFIG = "kafka.encoding";
  public static final String KAFKA_PARAMETER_CONFIG_PREFIX = "kafka.parameter.";

  private static final String KUDU_MASTERS_CONFIG = "kudu.masters";
  private static final String KUDU_TABLE_CONFIG = "kudu.table";

  private static void assertConfig(Config config, String key) {
    if (!config.hasPath(key)) {
      throw new RuntimeException("Missing required property [" + key + "]");
    }
  }

  private static SparkConf getSparkConfiguration(Config config) {
    SparkConf sparkConf = new SparkConf();

    if (config.hasPath(APPLICATION_NAME_PROPERTY)) {
      String applicationName = config.getString(APPLICATION_NAME_PROPERTY);
      sparkConf.setAppName(applicationName);
    }

    // Dynamic allocation should not be used for Spark Streaming jobs because the latencies
    // of the resource requests are too long.
    sparkConf.set("spark.dynamicAllocation.enabled", "false");
    // Spark Streaming back-pressure helps automatically tune the size of the micro-batches so
    // that they don't breach the micro-batch length.
    sparkConf.set("spark.streaming.backpressure.enabled", "true");
    // Rate limit the micro-batches when using Apache Kafka to 2000 records per Kafka topic partition
    // per second. Without this we could end up with arbitrarily large initial micro-batches
    // for existing topics.
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "2000");
    // Override the Spark SQL shuffle partitions with the default number of cores. Otherwise
    // the default is typically 200 partitions, which is very high for micro-batches.
    sparkConf.set("spark.sql.shuffle.partitions", "2");
    // Override the caching of KafkaConsumers which has been shown to be problematic with multi-core executors
    // (see SPARK-19185)
    sparkConf.set("spark.streaming.kafka.consumer.cache.enabled", "false");
//      sparkConf.set("spark.sql.catalogImplementation", "in-memory");
//      sparkConf.set("spark.sql.shuffle.partitions", "1");
//      sparkConf.set("spark.sql.warehouse.dir", "target/spark-warehouse");

    if (config.hasPath(NUM_EXECUTORS_PROPERTY)) {
      sparkConf.set("spark.executor.instances", config.getString(NUM_EXECUTORS_PROPERTY));
    }

    if (config.hasPath(NUM_EXECUTOR_CORES_PROPERTY)) {
      sparkConf.set("spark.executor.cores", config.getString(NUM_EXECUTOR_CORES_PROPERTY));
    }

    if (config.hasPath(EXECUTOR_MEMORY_PROPERTY)) {
      sparkConf.set("spark.executor.memory", config.getString(EXECUTOR_MEMORY_PROPERTY));
    }

    // Override the Spark SQL shuffle partitions with the number of cores, if known.
    if (config.hasPath(NUM_EXECUTORS_PROPERTY) && config.hasPath(NUM_EXECUTOR_CORES_PROPERTY)) {
      int executors = config.getInt(NUM_EXECUTORS_PROPERTY);
      int executorCores = config.getInt(NUM_EXECUTOR_CORES_PROPERTY);
      Integer shufflePartitions = executors * executorCores;

      sparkConf.set("spark.sql.shuffle.partitions", shufflePartitions.toString());
    }

    // Allow the user to provide any Spark configuration and we will just pass it on. These can
    // also override any of the configurations above.
    if (config.hasPath(SPARK_CONF_PROPERTY_PREFIX)) {
      Config sparkConfigs = config.getConfig(SPARK_CONF_PROPERTY_PREFIX);
      for (Map.Entry<String, ConfigValue> entry : sparkConfigs.entrySet()) {
        String param = entry.getKey();
        String value = entry.getValue().unwrapped().toString();
        if (value != null) {
          sparkConf.set(param, value);
        }
      }
    }

    String mohitConf = config.root().render(ConfigRenderOptions.concise());
    sparkConf.set("mohit.configuration", mohitConf);

    return sparkConf;
  }

  private static synchronized SparkSession getSparkSession(Config config) {
    SparkConf sparkConf = getSparkConfiguration(config);
    if (!sparkConf.contains("spark.master")) {
      LOG.warn("Spark master not provided, instead using local mode");
      sparkConf.setMaster("local[*]");
    }

    if (!sparkConf.contains("spark.app.name")) {
      LOG.warn("Spark application name not provided, instead using empty string");
      sparkConf.setAppName("");
    }

    SparkSession.Builder sparkSessionBuilder = SparkSession.builder();
    if (!config.hasPath(SPARK_SESSION_ENABLE_HIVE_SUPPORT) ||
        config.getBoolean(SPARK_SESSION_ENABLE_HIVE_SUPPORT)) {
      sparkSessionBuilder.enableHiveSupport();
    }

    return sparkSessionBuilder.config(sparkConf).getOrCreate();
  }

  private static synchronized JavaStreamingContext getJavaStreamingContext(
      SparkSession ss, Config config) {
    int batchMilliseconds = config.getInt(BATCH_MILLISECONDS_PROPERTY);
    final Duration batchDuration = Durations.milliseconds(batchMilliseconds);
    return new JavaStreamingContext(
        new JavaSparkContext(ss.sparkContext()), batchDuration);
  }

  private static void addCustomKafkaParams(Map<String, Object> params, Config config) {
    for (Map.Entry<String, ConfigValue> entry : config.entrySet()) {
      String propertyName = entry.getKey();
      if (propertyName.startsWith(KAFKA_PARAMETER_CONFIG_PREFIX)) {
        String paramName = propertyName.substring(KAFKA_PARAMETER_CONFIG_PREFIX.length());
        String paramValue = config.getString(propertyName);

        if (LOG.isDebugEnabled()) {
          LOG.debug("Adding Kafka property: {} = \"{}\"", paramName, paramValue);
        }
        params.put(paramName, paramValue);
      }
    }
  }

  public static JavaDStream<?> getKafkaDStream(JavaStreamingContext jssc,
                                          String groupID,
                                          Config config) throws Exception {
    assertConfig(config, KAFKA_BROKERS_CONFIG);
    assertConfig(config, KAFKA_TOPIC_CONFIG);

    Map<String, Object> kafkaParams = Maps.newHashMap();
    String brokers = config.getString(KAFKA_BROKERS_CONFIG);
    kafkaParams.put("bootstrap.servers", brokers);
    List<String> topics = config.getStringList(KAFKA_TOPIC_CONFIG);
    Set<String> topicSet = Sets.newHashSet(topics);
    String encoding = config.getString(KAFKA_ENCODING_CONFIG);
    if (encoding.equals("string")) {
      kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
      kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    }
    else if (encoding.equals("bytearray")) {
      kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
      kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    }
    else {
      throw new RuntimeException("Invalid Kafka input encoding type. Valid types are 'string' and 'bytearray'.");
    }

    kafkaParams.put("group.id", groupID);
    kafkaParams.put("enable.auto.commit", "false");
    addCustomKafkaParams(kafkaParams, config);
    JavaDStream<?> dStream = KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(),
        ConsumerStrategies.<String, String>Subscribe(topicSet, kafkaParams));
    return dStream;
  }

  public static void runStreaming(Config config) throws Exception {
    assertConfig(config, KAFKA_GROUP_ID_CONFIG);
    final String groupID = config.getString(KAFKA_GROUP_ID_CONFIG);
    assertConfig(config, KUDU_MASTERS_CONFIG);
    final String kuduMasters = config.getString(KUDU_MASTERS_CONFIG);
    assertConfig(config, KUDU_TABLE_CONFIG);
    final String kuduTableName = config.getString(KUDU_TABLE_CONFIG);
    assertConfig(config, WINDOW_MILLISECONDS_PROPERTY);
    int windowMilliseconds = config.getInt(WINDOW_MILLISECONDS_PROPERTY);

    final SparkSession spark = getSparkSession(config);
    JavaStreamingContext ssc = getJavaStreamingContext(spark, config);
    JavaDStream dstream = getKafkaDStream(ssc, groupID, config);
    JavaPairDStream<String, String> pairDStream =
        dstream.mapToPair(new PairFunction<ConsumerRecord<String,String>, String, String>() {
          @Override
          public Tuple2<String, String> call(ConsumerRecord<String,String> recObject)
              throws Exception {
            return new Tuple2<String, String>(recObject.key(), recObject.value());
          }
        });

    final KuduContext kuduContext   = new KuduContext(kuduMasters, spark.sparkContext());

    JavaPairDStream<String, String> windowedStream = pairDStream.window(
        new Duration(windowMilliseconds));
    windowedStream.foreachRDD(new VoidFunction<JavaPairRDD<String, String>>() {
      @Override
      public void call(JavaPairRDD<String, String> rdd) throws Exception {
        JavaRDD<Row> fieldsRdd = rdd.map(new Function<Tuple2<String,String>, Row>() {
          @Override
          public Row call(Tuple2<String, String> rec) {
            String[] flds    = rec._2().split(",");
            Long measure     = Long.parseLong(flds[0]);
            Integer vehicles = Integer.parseInt(flds[1].trim());
            return RowFactory.create(measure, vehicles);
          }
        });

        StructType schema = DataTypes.createStructType(new StructField[] {
            DataTypes.createStructField("measurement_time", DataTypes.LongType, false),
            DataTypes.createStructField("number_of_vehicles", DataTypes.IntegerType, true)
        });

        Dataset<Row> dataFrame = spark.createDataFrame(fieldsRdd,schema);
        dataFrame.registerTempTable("traffic");

        String query = "SELECT UNIX_TIMESTAMP() * 1000 as_of_time,"           +
            "       ROUND(AVG(number_of_vehicles),2) avg_num_veh,"  +
            "       MIN(number_of_vehicles) min_num_veh,"           +
            "       MAX(number_of_vehicles) max_num_veh,"           +
            "       MIN(measurement_time) first_meas_time,"         +
            "       MAX(measurement_time) last_meas_time "          +
            "   FROM traffic";
        Dataset resultsDataFrame = spark.sql(query);

          /* NOTE: All 3 methods provided are equivalent UPSERT operations on the
                   Kudu table and are idempotent, so we can run all 3 in this example
                   (although only 1 is necessary) */

        // Method 1: All kudu operations can be used with KuduContext (INSERT, INSERT IGNORE,
        //           UPSERT, UPDATE, DELETE)
//         kuduContext.upsertRows(resultsDataFrame, kuduTableName);

        // Method 2: The DataFrames API  provides the 'write' function (results
        //           in a Kudu UPSERT)
        final Map<String, String> kuduOptions = new HashMap<>();
        kuduOptions.put("kudu.master", kuduMasters);
        kuduOptions.put("kudu.table", "impala::" + kuduTableName);
        resultsDataFrame.write().format("org.apache.kudu.spark.kudu").options(kuduOptions).mode("append").save();

        // Method 3: A SQL INSERT through SQLContext also results in a Kudu UPSERT
//                   resultsDataFrame.registerTempTable("traffic_results");
//                   spark.read().format("org.apache.kudu.spark.kudu").options(kuduOptions).load()
//                        .registerTempTable(kuduTableName);
//                   spark.sql("INSERT INTO TABLE `" + kuduTableName + "` SELECT * FROM traffic_results");
      }
    });

    ssc.start();
    ssc.awaitTermination();
  }
}