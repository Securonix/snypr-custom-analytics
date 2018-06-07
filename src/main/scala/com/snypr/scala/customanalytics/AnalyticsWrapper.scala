package com.snypr.scala.customanalytics

import java.util.Properties

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger

import com.securonix.application.common.EventCompressor
import com.securonix.kafkaclient.producers.EEOProducer
import com.securonix.kafkaclient.producers.KafkaProducerFactory
import com.securonix.kafkaclient.producers.KafkaProducerFactory.TYPE_OF_MESSAGE
import com.securonix.snyper.common.JSONUtil
import com.securonix.snyper.config.beans.HadoopConfigBean.KAFKA_SOURCE
import com.securonix.snyper.config.beans.KafkaConfigBean

import collection.JavaConverters._
import com.securonix.snyper.common.EnrichedEventObject
import java.util.ArrayList

/**
 * AnalyticsWrapper : Class to process/Execute custom Analytics.
 */
class AnalyticsWrapper {

  val LOGGER: Logger = LogManager.getLogger();

  /**
   * proccessBytes()
   * messages - in byte format
   * kafkaConfigBean - configuration for the kafka params
   */
  def proccessBytes(messages: Array[Byte], kafkaConfigBean: KafkaConfigBean) {
    try {
      val props: Properties = new Properties();
      props.put("source", KAFKA_SOURCE.CLUSTER);

      val eeoProducer: EEOProducer = KafkaProducerFactory.INSTANCE.getProducer(TYPE_OF_MESSAGE.EEO, kafkaConfigBean, props).asInstanceOf[EEOProducer];

      LOGGER.info("Uncompressing the message :" + messages.length)

      // Provoide the Compression type: Ex- gzip , snappy , none and etc
      val json: String = EventCompressor.uncompressString(messages, kafkaConfigBean.getCompressionType);

      if (json != null) {
        var list: ArrayList[EnrichedEventObject] = JSONUtil.fromJSON(json).asInstanceOf[ArrayList[EnrichedEventObject]];
        
    
        list.asScala.toList.foreach { (x: EnrichedEventObject) =>

          x.setViolations(x.getCustomViolations)

        }

        /**
         * Apply the Custom Analytics on List of EEO's and publish messages to Destination topic , Ex : Violation Topic
         */

        // Send the messages to Destination topic, Ex: Violation Topic
        eeoProducer.publish(list, kafkaConfigBean.getViolationTopic)

      }
    } catch {
      case e: Exception =>
        {
          LOGGER.error("***Exception while un compressing byte data***", e)
        }
    }

  }
}