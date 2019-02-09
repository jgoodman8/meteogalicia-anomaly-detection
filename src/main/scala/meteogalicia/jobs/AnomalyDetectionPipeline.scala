package meteogalicia.jobs

import meteogalicia.model.PollutionGases
import meteogalicia.services.ConfigurationService.Kafka
import meteogalicia.services.kafka.{MessagesConsumer, MessagesProducer}
import meteogalicia.services.{ConfigurationService, MLService}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

object AnomalyDetectionPipeline extends App {

  def configPipeline(streamingContext: StreamingContext): Unit = {

    val model: Broadcast[KMeansModel] = streamingContext.sparkContext.broadcast(MLService.loadModel(sparkSession))
    val threshold: Broadcast[Double] = streamingContext.sparkContext.broadcast(MLService.loadThreshold(sparkSession))

    val kafkaBrokers: Broadcast[String] = streamingContext.sparkContext.broadcast(ConfigurationService.Kafka.getBroker)

    val kafkaFeed: DStream[(String, String)] = MessagesConsumer
      .connectToKafka(
        streamingContext,
        ConfigurationService.Kafka.getZookeeper,
        ConfigurationService.Kafka.getGroup,
        ConfigurationService.Kafka.getConsumerTopic,
        ConfigurationService.Kafka.getNumThreads
      )

    val dataStream: DStream[(String, PollutionGases)] = MessagesConsumer.getPollutionStream(kafkaFeed)

    val anomaliesStream: DStream[(String, PollutionGases)] = MLService
      .detectAnomalies(sparkSession, dataStream, model.value, threshold.value)

    anomaliesStream.foreachRDD(anomaliesRDD => {
      MessagesProducer.publishAnomalies(anomaliesRDD, kafkaBrokers, Kafka.getProducerTopic)
    })
  }

  val sparkSession = SparkSession.builder().appName("Anomaly detection").getOrCreate()

  val batch_interval_ms = Milliseconds(ConfigurationService.getBatchInterval)
  val sparkStreamingContext = new StreamingContext(sparkSession.sparkContext, batch_interval_ms)

  configPipeline(sparkStreamingContext)

  sparkStreamingContext.start()
  sparkStreamingContext.awaitTermination()

  sparkSession.stop()
}
