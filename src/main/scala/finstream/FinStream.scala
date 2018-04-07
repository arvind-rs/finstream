
/*
 * Data pipeline to collect, transform and store financial price and social media text data
 * for a given list of stocks using Spark streaming.
 * @author: arvind-rs
 * @date: 03/30/2018
 */

/*
 * Things to do:
 * 1. Build Fetcher Receiver X
 * 2. Retrofit the scraper code to work with FinStream X
 * 3. Add capabilities to crawl and scrape JSON data from APIs
 * 4. Transform the data into DataFrames
 * 5. Add MySQL db connections
 * 6. Write the data to the MySQL tables
 */

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._ 
import org.apache.log4j.{Level, Logger}
import finstream.receivers._
import finstream.scraper._
import finstream.utils._

object FinStream {

	val masterConfig = "conf/master.conf"
	val scrapeConfigPath = UtilMethods.getMasterConfig.getString("SCRAPE_CONFIG")
	val scrapeConfig = UtilMethods.loadScrapeConfig(scrapeConfigPath)(0)

	val urlList = List("https://www.google.com/search?q=FB", "https://www.google.com/search?q=AMZN",
		"https://www.google.com/search?q=GOOG")
	//val urlList = List("https://www.google.com/search?q=FB")

	def main(args: Array[String]) {

		Logger.getLogger("org").setLevel(Level.ERROR)

		val sparkConf = new SparkConf().setMaster("local[4]").setAppName("Test")
		val ssc = new StreamingContext(sparkConf, Seconds(60))

		val fetcherService = ssc.receiverStream(new Fetcher(urlList))
		println("Got data ...")
		//fetcherService.saveAsTextFiles("output/test1","")

		//fetcherService.map(_.contentType).print()
		val cut: String => String = (s: String) => {s.substring(s.indexOf("W0pUAc fmob_pr fac-l"),s.indexOf("W0pUAc fmob_pr fac-l")+100)}
		//fetcherService.map(x => (x.contentType, 1)).reduceByKey(_ + _).print()
		fetcherService.map(x => Scraper.scrapeHTML(x.response, scrapeConfig)).print()
		println(scrapeConfig)
		/*val lines = ssc.socketTextStream("localhost",9999)

		val words = lines.flatMap(_.split(" "))
		val pairs = words.map((_, 1))
		val wordCount = pairs.reduceByKey(_ + _)

		//wordCount.print()

		//wordCount.saveAsTextFiles("output/test","")*/

		ssc.start()
		ssc.awaitTermination()
	}
}