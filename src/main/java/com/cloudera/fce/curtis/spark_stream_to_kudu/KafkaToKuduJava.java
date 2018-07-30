package com.cloudera.fce.curtis.spark_stream_to_kudu;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaToKuduJava {
  private static Logger LOG = LoggerFactory.getLogger(KafkaToKuduJava.class);

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      LOG.error("Configuration file missing");
    }

    Config config = ConfigFactory.parseFile(new File(args[0]));
    SparkStreamingRunner.runStreaming(config);
 }
}