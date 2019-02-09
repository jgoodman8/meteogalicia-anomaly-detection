package meteogalicia.services.kafka

import meteogalicia.model.PollutionGases
import meteogalicia.services.{ConfigurationService, Utils}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils

object MessagesConsumer {

  def connectToKafka(streamingContext: StreamingContext, zookeeperQuorum: String, groupId: String, topics: String,
                     numThreads: Int): DStream[(String, String)] = {

    streamingContext.checkpoint(ConfigurationService.getCheckpointDir)

    val topicMap = topics.split(",").map((_, numThreads)).toMap

    KafkaUtils.createStream(streamingContext, zookeeperQuorum, groupId, topicMap)
  }

  def getPollutionStream(kafkaFeed: DStream[(String, String)]): DStream[(String, PollutionGases)] = {
    val pollutionStream = kafkaFeed.transform(inputRDD => {

      inputRDD.map(input => {
        println(input._2)
        Utils.parsePollutionInput(input._2)
      })
    })

    pollutionStream
  }
}
