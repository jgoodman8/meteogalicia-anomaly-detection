package meteogalicia.services

import com.typesafe.config.{Config, ConfigFactory}

object ConfigurationService {
  val configuration: Config = ConfigFactory.load("application.conf")

  def getThresholdPath: String = configuration.getString("thresholdPath")

  def getModelPath: String = configuration.getString("modelPath")

  def getPollutionCSVPath: String = configuration.getString("pollutionPath")

  def getBatchInterval: Int = configuration.getInt("Streaming.batch_interval_ms")

  def getCheckpointDir: String = configuration.getString("Streaming.checkpoint_dir")

  object Kafka {
    def getProducerTopic: String = configuration.getString("Streaming.output_topic")

    def getConsumerTopic: String = configuration.getString("Streaming.input_topic")

    def getZookeeper: String = configuration.getString("Streaming.zookeeper_cluster")

    def getGroup: String = configuration.getString("Streaming.group")

    def getBroker: String = configuration.getString("Streaming.brokers")

    def getNumThreads: Int = configuration.getInt("Streaming.num_threads")
  }

}
