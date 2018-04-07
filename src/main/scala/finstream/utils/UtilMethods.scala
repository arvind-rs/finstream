package finstream.utils

/*
 * Collection of utility methods used in various parts of the application.
 * @author: arvind-rs
 * @date: 04/01/2018
 */

import java.io._
import org.json._
import scala.collection.mutable.ListBuffer
import com.typesafe.config.{ Config, ConfigFactory }

object UtilMethods {

	val masterConfig = "conf/master.conf"

	// Method to test if a given URL is a valid URL or not
	def isValidUrl(url: String): Boolean = {
		if(url.startsWith("http://") || url.startsWith("https://"))
			return true
		return false
	}

	// Method to get an instance of the Master config file
  	def getMasterConfig(): Config = {
    	return ConfigFactory.parseFile(new File(masterConfig))
	}

	// Method to open the file in the given file path and return its content
  	def openFile(filePath: String): String = {
  		var bufferedSource: scala.io.BufferedSource = null
    	try {
      		bufferedSource = scala.io.Source.fromFile(filePath)
      		return bufferedSource.getLines.mkString
    	} catch {
      		case ex: java.io.FileNotFoundException => throw new RuntimeException(ex)
    	} finally {
    		if(bufferedSource != null) bufferedSource.close()
    	}
	}

	// Method to load the scrape config to be used
	def loadScrapeConfig(configName: String): List[ScrapeConfig] = {
		val configList = new ListBuffer[ScrapeConfig]
      	val filePath = UtilMethods.getMasterConfig().getString("SCRAPER_CONFIG_FOLDER") + configName
      	val fileContents = try { UtilMethods.openFile(filePath) } catch { case ex: RuntimeException => throw new RuntimeException("Exception occurred while reading the file. Please check if the file exists and has read permissions." + ex) }

      	try {
        	// Parse the JSON config file and get the parameters for the scraper configuration
        	val obj = new JSONObject(fileContents)

        	val useCaseArrayLength = obj.getJSONObject("config").getJSONArray("use_cases").length()
        	for (i <- 0 until useCaseArrayLength) {
          		// Get the use case name
          		val useCase = obj.getJSONObject("config").getJSONArray("use_cases").get(i).toString()
          		// Get the classifier to be used by the ScraperEngine
          		val classifierXpath = obj.getJSONObject("config").getJSONObject(useCase).getString("classifier")
          		// Get the Xpaths list
          		val jsonXpathList = obj.getJSONObject("config").getJSONObject(useCase).getJSONObject("xpath_list")
          		val xpathList = UtilMethods.jsonXpathListDeserialize(jsonXpathList)
          		// Get the normalizer regex patterns list
          		val jsonPatternsList = obj.getJSONObject("config").getJSONObject(useCase).getJSONObject("normalizers")
          		val normalizerList = UtilMethods.jsonRegexListDeserialize(jsonPatternsList)

          		configList += ScrapeConfig(useCase, XpathMap("classifier", classifierXpath), xpathList, normalizerList)
        	}
        	return configList.toList

      	} catch {
        	case ex: Exception => {
        		throw new RuntimeException("Exception occurred while processing JSON scrape config. Please check if the format is correct." + ex)
        	} 
		}
	}

	// Method to convert a JSON Xpath Object list to a Scala collections list
  	def jsonXpathListDeserialize(jsonList: JSONObject): List[XpathMap] = {
    	val map = jsonList.toMap()
    	val keyList = map.keySet()
    	val iterator = keyList.iterator()
    	val listBuffer = ListBuffer[XpathMap]()
    	while (iterator.hasNext()) {
      		val keyName = iterator.next()
      		listBuffer += XpathMap(keyName, map.get(keyName).toString())
    	}
    	return listBuffer.toList
  	}

  	// Method to convert a JSON Regex Object list to a Scala collections list
  	def jsonRegexListDeserialize(jsonList: JSONObject): List[RegexPatternMap] = {
    	val map = jsonList.toMap()
    	val keyList = map.keySet()
    	val iterator = keyList.iterator()
    	val listBuffer = ListBuffer[RegexPatternMap]()
    	while (iterator.hasNext()) {
      		val keyName = iterator.next()
      		listBuffer += RegexPatternMap(keyName, map.get(keyName).toString())
    	}
    	return listBuffer.toList
	}
}