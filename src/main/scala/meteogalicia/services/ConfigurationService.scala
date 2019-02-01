package meteogalicia.services

import com.typesafe.config.{Config, ConfigFactory}

object ConfigurationService {
  val configuration: Config = ConfigFactory.load("application.conf")

  def getThresholdPath: String = configuration.getString("thresholdPath")

  def getModelPath: String = configuration.getString("modelPath")

  def getPollutionCSVPath: String = configuration.getString("pollutionPath")
}
