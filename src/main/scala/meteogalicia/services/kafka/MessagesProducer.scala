package meteogalicia.services.kafka

import java.util

import meteogalicia.model.PollutionGases
import meteogalicia.services.Utils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object MessagesProducer {

  def publishAnomalies(rdd: RDD[(String, PollutionGases)], kafkaBrokers: Broadcast[String], topic: String): Unit = {
    rdd.foreachPartition(partition => {
      val kafkaConfiguration = getKafkaConfiguration(kafkaBrokers.value)
      val producer = new KafkaProducer[String, String](kafkaConfiguration)

      partition.foreach(record => {
        val message = Utils.pollution_to_string(record._2, record._1)
        println(message)
        producer.send(new ProducerRecord[String, String](topic, record._1, message))
      })

      producer.close()
    })
  }

  def getKafkaConfiguration(brokers: String): util.HashMap[String, Object] = {
    val properties = new util.HashMap[String, Object]()

    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    properties
      .put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    properties
  }
}
