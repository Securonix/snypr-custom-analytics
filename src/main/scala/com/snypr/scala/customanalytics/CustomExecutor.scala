package com.snypr.scala.customanalytics
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import java.util.Properties;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.config.SslConfigs
import org.apache.spark.streaming.StreamingContext

import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.ConsumerStrategies
import org.apache.spark.streaming.kafka010.LocationStrategies

import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.dstream.DStream

import org.apache.spark.rdd.RDD
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.securonix.kafka.CustomStringDecoder
import com.securonix.kafka.CustomDefaultDecoder
import com.securonix.hbaseutil.HBaseClient

import com.securonix.snyper.policy.beans.violations.Violation
import com.securonix.hbaseutil.dao.KafkaOffsetDAO
import com.securonix.snyper.common.EnrichedEventObject
import com.securonix.snyper.config.beans.HadoopConfigBean
import com.securonix.snyper.config.beans.KafkaConfigBean
import com.securonix.snyper.config.beans.SSLConfigBean

import com.securonix.snyper.config.beans.HBaseConfigBean
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

import com.securonix.kafkaclient.producers.KafkaProducer
import com.securonix.snyper.config.beans.SparkConfigBean
import com.securonix.snyper.config.beans.SolrConfigBean
import com.securonix.snyper.config.beans.TenantConfigBean
import com.securonix.snyper.config.beans.SparkRunConfigBean
import com.securonix.snyper.config.beans.HDFSConfigBean
import com.securonix.snyper.config.beans.ImpalaConfigBean
import com.securonix.snyper.config.beans.RedisConfigBean
import com.securonix.snyper.config.beans.YarnConfigBean
import com.securonix.snyper.config.beans.ZookeeperConfigBean
import com.securonix.application.hadoop.HadoopConfigUtil

object CustomExecutor {

  val LOGGER: Logger = LogManager.getLogger();

  import com.securonix.kafka.KafkaOffsetManager
  /**
   * Offset manager :It Manages the  consuming the messages by earliest , latest or stored offsets
   */
  var offsetManager: KafkaOffsetManager = null;

  /**
   * Job to execute spark custom analytics.
   */
  def main(args: Array[String]) {
    import org.apache.logging.log4j.LogManager
    import org.apache.log4j.{ Level, Logger }

    val argumentsMap = extractArguments(args)

    /**
     * cg - consumer group and -d - duration interval is mandatory params.
     */
    if (!argumentsMap.containsKey("-cg") || !argumentsMap.containsKey("-d")) {

      System.err.println("\nERROR: Insufficient input, consumer group and duration are mandatory!");
      System.err.println("\nMandatory:");
      System.err.println("\t-cg\t\tConsumer group");
      System.err.println("\t-d\t\tDuration (in seconds)");
      System.err.println("Optional:");
      System.err.println("\t-or\t\tAuto Offset Reset [smallest|largest]");
      System.err.println("\t-mrpp\t\tMax rate per partition");
      System.err.println(); // a blank line!
      System.exit(-1);
    }

    val consumerGroupId: String = argumentsMap.get("-cg");
    val streamingDuration: Int = Integer.parseInt(argumentsMap.get("-d"));

    var autoOffsetReset: String = null;
    if (argumentsMap.containsKey("-or")) {
      autoOffsetReset = argumentsMap.get("-or").toLowerCase();
    }

    var maxRatePerPartition: String = null;
    if (argumentsMap.containsKey("-mrpp")) {
      maxRatePerPartition = argumentsMap.get("-mrpp");
    }
    LOGGER.debug("Max Rate Per Partition- {}", maxRatePerPartition);

    LOGGER.debug("Loading Hadoop Configuration !!!");

    //load the Hadoop configuration from the configxml -> HADOOP_CONFIG
    val hcb: HadoopConfigBean = HadoopConfigUtil.getHadoopConfiguration()
    val kafkaConfigBean: KafkaConfigBean = hcb.getKafkaConfigBean
    val hBaseConfigBean: HBaseConfigBean = hcb.gethBaseConfigBean

    /**
     * Run the job in Cluster Mode
     */
    val streamingConf: SparkConf = new SparkConf();

    /**
     * Topic to consume the Messages from  , for ex: TiER2 topic
     */
    var topic: String = kafkaConfigBean.getTier2Topic;
    //var topic: String = kafkaConfigBean.getEnrichedTopic;

    if (maxRatePerPartition != null && !maxRatePerPartition.isEmpty()) {
      streamingConf.set("spark.streaming.kafka.maxRatePerPartition", maxRatePerPartition);
    }

    /**
     * Specify the below properties if serialization type is Kryo
     */
    streamingConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    streamingConf.set("spark.kryo.registrationRequired", "true");

    streamingConf.registerKryoClasses(Array(classOf[scala.Tuple3[Any, Any, Any]], classOf[Properties], classOf[SSLConfigBean], classOf[Array[Byte]], classOf[ConcurrentHashMap[String, String]], classOf[Violation],
      classOf[HashSet[String]], classOf[HashMap[String, String]], classOf[ZookeeperConfigBean], classOf[HDFSConfigBean], classOf[HBaseConfigBean], classOf[ImpalaConfigBean], classOf[KafkaConfigBean], classOf[SparkRunConfigBean], classOf[SolrConfigBean], classOf[YarnConfigBean], classOf[SparkConfigBean], classOf[RedisConfigBean], classOf[TenantConfigBean], classOf[HadoopConfigBean.AUTHENTICATION_TYPE], classOf[HadoopConfigBean.ENVIRONMENT], classOf[HadoopConfigBean.KAFKA_SOURCE], classOf[HadoopConfigBean.KAFKA_SOURCE], classOf[ArrayList[String]], classOf[LinkedHashMap[String, String]]));

    val topicsSet: Set[String] = new HashSet(Arrays.asList(topic));

    val kafkaParams: Map[String, Object] = new HashMap[String, Object]();
    kafkaParams.put("group.id", consumerGroupId);
    kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfigBean.getBrokers());

    if (kafkaConfigBean.getMaxMessageSize() > 0) {
      kafkaParams.put("fetch.message.max.bytes", "" + kafkaConfigBean.getMaxMessageSize());
    }

    val authType: HadoopConfigBean.AUTHENTICATION_TYPE = kafkaConfigBean.getAuthType();
    val src: HadoopConfigBean.KAFKA_SOURCE = HadoopConfigBean.KAFKA_SOURCE.CLUSTER;

    import com.securonix.kafkaclient.KafkaUtil

    KafkaUtil.addSSLProperties(kafkaParams, kafkaConfigBean, authType, src);

    /**
     * Initialize the Spark Streaming Context.
     * Duration : in Milliseconds, ex: 1000
     */
    val jssc = new StreamingContext(streamingConf, Milliseconds(streamingDuration));

    kafkaParams.put("key.deserializer", classOf[CustomStringDecoder]);
    kafkaParams.put("value.deserializer", classOf[CustomDefaultDecoder]);

    /**
     * HBaseClient: Used to store/pickup the offsets.
     */
    LOGGER.debug("Initializing HBase client ..");
    val hbaseClient: HBaseClient = new HBaseClient();
    hbaseClient.initializeHBase(hBaseConfigBean)

    LOGGER.debug("HBase initialized!!!")

    /**
     * offsetManager: Manages the storing /loading the offsets into/from the HBase.
     */
    offsetManager = new KafkaOffsetManager(hcb.getKafkaConfigBean(), new KafkaOffsetDAO(hbaseClient), consumerGroupId, topic, HadoopConfigBean.KAFKA_SOURCE.CLUSTER);

    LOGGER.info("Auto Offset reset is: " + autoOffsetReset)
    /**
     * dStream : Create the DStream to receive the messages
     */
    val dStream: InputDStream[ConsumerRecord[String, Array[Byte]]] =
      KafkaUtils.createDirectStream(
        jssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, Array[Byte]](Arrays.asList(topic), kafkaParams.asInstanceOf[java.util.Map[String, Object]], offsetManager.findOffsets(autoOffsetReset)))

    /**
     * jssc - Spark Streaming context
     * dStream - to Consume the Messages
     * offsetManager - earliest/ latest/ offsets from HBase
     * hcb - Hadoop Configuration
     */
    val sparkCustomReceiver = new CustomAnalyticsReceiver(jssc, dStream, offsetManager, hcb)
    sparkCustomReceiver.executeCustomSparkJob()
  }

  /**
   * Extract the user Arguments.
   */
  def extractArguments(args: Array[String]): Map[String, String] = {
    val map: Map[String, String] = new HashMap();
    var arg: String = null;
    var arr: Array[String] = null;

    for (i <- 0 to (args.length - 1)) {
      arg = args(i);
      arr = arg.split(":(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1)
      if (arr.length == 2) {
        map.put(arr(0), arr(1));
      }
    }
    LOGGER.info(map)
    map

  }

}