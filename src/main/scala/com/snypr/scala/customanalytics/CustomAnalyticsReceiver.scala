package com.snypr.scala.customanalytics
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream

import com.securonix.kafka.KafkaOffsetManager
import com.securonix.snyper.config.beans.HadoopConfigBean
import com.securonix.spark.dstream.SecuronixJavaInputDStream

/**
 * CustomAnalyticsReceiver - To consume the messages and process.
 * jssc - Spark Streaming context
 * dStream - to Consume the Messages
 * offsetManager - earliest/ latest/ offsets from HBase
 * hcb - Hadoop Configuration
 */
class CustomAnalyticsReceiver (val jssc: StreamingContext, val dStream: InputDStream[ConsumerRecord[String, Array[Byte]]], val offsetManager: KafkaOffsetManager, val hcb: HadoopConfigBean) {
   val LOGGER: Logger = LogManager.getLogger();

  /**
   * executeSparkJob() - to Consume and process Messages from the DStream.
   */
  def executeCustomSparkJob() {
    import org.apache.log4j.{ Level, Logger }

    /**
     * Broadcast the KafkaConfigBean and Compression Type from Driver to Executor.
     */
    val kafkaConfigBean = jssc.sparkContext.broadcast(hcb.getKafkaConfigBean)

    try {
      /**
       * SecuronixJavaInputDStream - Class the Save the offsets into HBase.
       */
      val securonixJavaInputDStream: SecuronixJavaInputDStream = new SecuronixJavaInputDStream();

      import org.apache.log4j.Logger
      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)

      LOGGER.info("Compression Type is :" + hcb.getKafkaConfigBean.getCompressionType)

      /**
       * Execute and Process the Messages parallel.
       */
      dStream.foreachRDD(rdd => {
        if (!rdd.isEmpty()) {
          try {

            rdd.foreachPartition((partitionOfRecords: Iterator[ConsumerRecord[String, Array[Byte]]]) => {

              /**
               * Map the values , For Ex: Array[Byte] messages
               */
              val messages = partitionOfRecords.map(f => f.value())

              /**
               * Wrapper to perform Custom Analytics on Messages
               */
              val process: AnalyticsWrapper = new AnalyticsWrapper()
              messages.foreach { (x: Array[Byte]) =>
                process.proccessBytes(x, kafkaConfigBean.value)
              }
            })

            LOGGER.debug("Saving  Offsets to HBase")

            /**
             * Save the offsets in HBase once the Events are processed.
             *
             */
            securonixJavaInputDStream.saveOffsets(offsetManager)
          } catch {
            case e: Exception =>
              {
                LOGGER.error("***Exception caught while processing RDD ***", e)
              }

          }
        }

      })

      jssc.start()

      try {
        jssc.awaitTermination()
        LOGGER.warn("*** Streaming terminated")
      } catch {
        case e: Exception => {
          LOGGER.error("*** Streaming exception while terminating and the exception is:", e)
        }
      }
    } catch {
      case e: Exception => {
        LOGGER.error("*** Streaming exception while processing the custom job and the exception is:", e)
      }
    }
  }
}