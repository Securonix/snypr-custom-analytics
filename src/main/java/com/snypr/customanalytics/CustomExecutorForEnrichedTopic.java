/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.snypr.customanalytics;

import com.securonix.application.common.EventCompressor;
import com.securonix.application.hadoop.HadoopConfigUtil;
import com.securonix.hbaseutil.HBaseClient;
import com.securonix.hbaseutil.dao.KafkaOffsetDAO;
import com.securonix.kafka.CustomDefaultDecoder;
import com.securonix.kafka.CustomStringDecoder;
import com.securonix.kafka.KafkaOffsetManager;
import com.securonix.kafkaclient.KafkaUtil;
import com.securonix.kafkaclient.producers.EEOProducer;
import com.securonix.kafkaclient.producers.KafkaProducerFactory;
import com.securonix.rdd.SecuronixRDD;
import com.securonix.snyper.common.EnrichedEventObject;
import com.securonix.snyper.common.JSONUtil;
import com.securonix.snyper.config.beans.HBaseConfigBean;
import com.securonix.snyper.config.beans.HadoopConfigBean;
import com.securonix.snyper.config.beans.KafkaConfigBean;
import com.securonix.snyper.policy.beans.violations.Violation;
import com.securonix.spark.dstream.SecuronixJavaInputDStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hbase.client.Put;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * <h1>Custom Spark Analyzer for SNYPR</h1>
 * The CustomExecutorForEnrichedTopic class provides an example to write a Spark Application
 * that can be launched within the SNYPR cluster to enable users to write their
 * own analytics. This CustomExecutorForEnrichedTopic class reads enriched & parsed events from
 * the Kafka Enriched topic and applies logic to identify violations. The
 * violations are published to the violation topic for downstream risk scoring.
 *
 * @see <code>publish</code> method in the
 * <code>com.securonix.kafkaclient.producers.EEOProducer</code> module
 * @author ManishKumar
 * @version 1.0
 * @since 2017-03-31
 */
public class CustomExecutorForEnrichedTopic implements Serializable {

    private final static Logger LOGGER = LogManager.getLogger();
    /**
     * Variable declared for Kafka offset management. The offset gets stored in
     * HBase
     *
     * @author ManishKumar
     * @version 1.0
     * @since 2017-03-31
     */
    private static KafkaOffsetManager offsetManager;

    /**
     * Main method for reading enriched events from Enriched Kafka
     * topic,uncompressing events, deserializing events and creating event
     * Object. Generates a RDD from the events and iterates to perform
     * analytics. Pushes violations to the Violation Topic.
     *
     * @param args Arguments passed to the main method by the Spark Submit
     * @throws java.lang.Exception
     * @author ManishKumar
     * @version 1.0
     * @since 2017-03-31
     * @see <code>publish</code> in the
     * <code>com.securonix.kafkaclient.producers.EEOProducer</code> module
     */
    public static void main(String args[]) throws Exception {
        // Extract arguments passed to Main Method
        final Map<String, String> argumentsMap = extractArguments(args);

        // Arguments must have consumer group and duration
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
        // Store consumergroup and duration in variables
        final String consumerGroupId = argumentsMap.get("-cg");
        final int streamingDuration = Integer.parseInt(argumentsMap.get("-d"));

        String autoOffsetReset = null;
        /////// Other Arguments that may be passed during Spark Submit ////////
        // Mandatory:
        //      -cg		Consumer group
        //      -d		Duration (in seconds)
        //  Optional:
        //      -or		Auto Offset Reset [smallest|largest]
        //      -rp		Repartition count
        //      -mrpp		Max rate per partition    
        ///////////////////////////////////////////////////////////////////////
        if (argumentsMap.containsKey("-or")) {
            autoOffsetReset = argumentsMap.get("-or").toLowerCase();
        }

        String maxRatePerPartition = null;
        if (argumentsMap.containsKey("-mrpp")) {
            maxRatePerPartition = argumentsMap.get("-mrpp");
        }
        LOGGER.debug("Max Rate Per Partition- {}", maxRatePerPartition);

        String topic = null;
        // Read Hadoop Settings from SNYPR database. This has all connection details for various hadoop components including Kafka & HBase
        //If Hadoop settings are not available, Spark job will fail to start up
        final HadoopConfigBean hcb = HadoopConfigUtil.getHadoopConfiguration();
        if (hcb == null) {
            System.err.println("Unable to obtain Hadoop configuration\n");
            System.exit(1);
        }

        // Extract Hbase connection details
        final HBaseConfigBean hbaseConfigBean = hcb.gethBaseConfigBean();
        if (hbaseConfigBean == null) {
            System.err.println("Unable to obtain HBase configuration\n");
            System.exit(-1);
        }

        // Extract Kafka connection details
        KafkaConfigBean kafkaConfigBean = hcb.getKafkaConfigBean();
        if (kafkaConfigBean == null) {
            System.err.println("\nERROR: Unable to obtain Kafka configuration\n");
            System.exit(-1);
        }

        final SparkConf sparkConf = new SparkConf();
        if (maxRatePerPartition != null && !maxRatePerPartition.isEmpty()) {
            sparkConf.set("spark.streaming.kafka.maxRatePerPartition", maxRatePerPartition);
        }

        // register class with Kryo for speed
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.registerKryoClasses(new Class[]{EnrichedEventObject.class, ConcurrentHashMap.class,CustomExecutorForEnrichedTopic.class, Violation.class, CustomExecutor.class,
            HashSet.class, HashMap.class, ArrayList.class, EnrichedEventObject[].class, Object[].class,
            org.apache.spark.sql.Row[].class, org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema.class,
            org.apache.spark.sql.types.StructType.class, org.apache.spark.sql.types.StructField[].class, org.apache.spark.sql.types.StructField.class,
            org.apache.spark.sql.types.BooleanType$.class, org.apache.spark.sql.types.Metadata.class, Put.class, scala.Tuple2[].class});
//        sparkConf.set("spark.kryo.registrationRequired", "true");

        // Extract Enriched Topic Name from configuration
        final Set<String> topicsSet = new HashSet<>(Arrays.asList(kafkaConfigBean.getEnrichedTopic().split(",")));
        if (topic == null || topic.isEmpty()) { // if topic is not provided on command line
            topic = topicsSet.iterator().next();
        }
        LOGGER.debug("Enriched topic- {}", topic);

        LOGGER.debug("Initializing HBase client ..");
        final HBaseClient hbaseClient = new HBaseClient();
        try {
            hbaseClient.initializeHBase(hbaseConfigBean);
            LOGGER.debug("HBase client initialized!");
        } catch (Exception ex) {
           LOGGER.error("\nERROR: Error initializing the job, exiting ..\n", ex);
            System.exit(-1);
        }
        LOGGER.debug("HBase and other stuff initialized!");
        // Set up Kafka paramaters
        final Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("group.id", consumerGroupId);
        kafkaParams.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfigBean.getBrokers());

        if (kafkaConfigBean.getMaxMessageSize() > 0) {
            kafkaParams.put("fetch.message.max.bytes", "" + kafkaConfigBean.getMaxMessageSize());
        }

        final HadoopConfigBean.AUTHENTICATION_TYPE authType = kafkaConfigBean.getAuthType();
        HadoopConfigBean.KAFKA_SOURCE src = HadoopConfigBean.KAFKA_SOURCE.CLUSTER;
        // Support for SSL in Kafka connection
        KafkaUtil.addSSLProperties(kafkaParams, kafkaConfigBean, authType, src);

        // Read from Kafka Enriched Topic using offset
        offsetManager = new KafkaOffsetManager(hcb.getKafkaConfigBean(), new KafkaOffsetDAO(hbaseClient), consumerGroupId, topic, HadoopConfigBean.KAFKA_SOURCE.CLUSTER);

        LOGGER.debug("Offset manager initialized!");

        // Create a RDD from events read from Enriched Topic
        final JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(streamingDuration));

        LOGGER.debug("About to create a direct stream ..");

        SecuronixJavaInputDStream securonixJavaInputDStream = new SecuronixJavaInputDStream();

        JavaDStream<byte[]> messages = securonixJavaInputDStream.createDirectStream(jssc,
                topic,
                String.class,
                byte[].class,
                CustomStringDecoder.class,
                CustomDefaultDecoder.class,
                byte[].class,
                kafkaParams,
                offsetManager,
                autoOffsetReset,
                (v1) -> v1.message());

        LOGGER.debug("Waiting for events .. CT-{} ET-{}", kafkaConfigBean.getCompressionType(), kafkaConfigBean.getEnrichedTopic());

        // Create RDD from each partitiion in Kafka Topic
        //  Iterate through RDD
        
        securonixJavaInputDStream.foreachRDD(messages,(JavaRDD<byte[]> rdd) -> {

            if (!rdd.isEmpty()) {
                final JavaRDD<EnrichedEventObject> listRdd = SecuronixRDD.mapPartitions(rdd, iterator -> {

                    final List<EnrichedEventObject> outgoingViolationEvent = new ArrayList<>();

                    iterator.forEachRemaining(b -> {

                        String json = null;
                        try {
                            // Uncompress Events
                            json = EventCompressor.uncompressString(b, kafkaConfigBean.getCompressionType());
                        } catch (Exception ex) {
                            LOGGER.error("Error in uncompressing", ex);
                        }
                        // Events are in JSON format and are converted to EEO object
                        List<EnrichedEventObject> list;
                        if (json != null) {
                            list = JSONUtil.fromJSON(json);
                            // Iterate through list of EEO in a batch
                            list.forEach(eeo -> {

                                Boolean isViolation = false;
                                // Initailize a list called violationContentList for storing violation events
                                List<Violation> violationContentList = new ArrayList<>();
                                //  Put your decision making logic here as follows
                                if (eeo.getRequesturl().contains("securonix")) {
                                    isViolation = true;
                                    // Initialize a Violation Object called violationContent and store violation event in this oject
                                    Violation violationContent = new Violation();
                                    // Set all violation information for violationContent
                                    violationContent.setPolicyId(9999l);

                                    // Add violationContent to violationContentList
                                    violationContentList.add(violationContent);
                                }
                                // If Violation was detected, send violations to Violation Topic
                                if (isViolation) {
                                    eeo.setViolations(violationContentList);
                                    outgoingViolationEvent.add(eeo);
                                    LOGGER.trace("Violations added # {}:{}", eeo.getViolations().size(), eeo.getCustomViolations().size());
                                }
                            }
                            );
                        }
                    });

                    return outgoingViolationEvent;
                }). persist(StorageLevel.MEMORY_ONLY_SER());

                Properties props = new Properties();
                props.put("source", HadoopConfigBean.KAFKA_SOURCE.CLUSTER);

                listRdd.foreachPartition((Iterator<EnrichedEventObject> iterator) -> {

                    final EEOProducer eeoProducer = (EEOProducer) KafkaProducerFactory.INSTANCE.getProducer(KafkaProducerFactory.TYPE_OF_MESSAGE.EEO, kafkaConfigBean, props);
                    final List<EnrichedEventObject> customViolations = new ArrayList<>();

                    // process individual events
                    iterator.forEachRemaining(eeo -> {

                        customViolations.add(eeo);
                    });

                    if (!customViolations.isEmpty()) {
                        LOGGER.trace("Publishing violations- CUSTOM # {}", customViolations.size());
                        eeoProducer.publish(customViolations, kafkaConfigBean.getViolationTopic());
                    }
                });

                securonixJavaInputDStream.saveOffsets(offsetManager);
            }

        });

        jssc.start();

        jssc.awaitTermination();

    }

    public static Map<String, String> extractArguments(String[] args) {

        final Map<String, String> map = new HashMap<>();
        String arg;
        String[] arr;

        for (int i = 0, max = args.length; i < max; i++) {
            arg = args[i];
            arr = arg.split(":(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);
            if (arr.length == 2) {
                map.put(arr[0], arr[1]);
            }
        }

        return map;
    }
}
