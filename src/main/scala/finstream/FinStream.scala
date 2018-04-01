
/*
 * Data pipeline to collect, transform and store financial price and social media text data
 * for a given list of stocks using Spark streaming.
 * @author: arvind-rs
 * @date: 03/31/2018
 */

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._ 
import org.apache.log4j.{Level, Logger}
import finstream.receivers._

object FinStream {

	val urlList = List("https://www.google.com/search?q=FB", "https://www.google.com/search?q=AMZN",
		"https://www.google.com/search?q=GOOG")
	//val urlList = List("https://www.google.com/search?q=FB")

	def main(args: Array[String]) {

		Logger.getLogger("org").setLevel(Level.ERROR)

		val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Test")
		val ssc = new StreamingContext(sparkConf, Seconds(5 * 60))

		val fetcherService = ssc.receiverStream(new Fetcher(urlList))

		//fetcherService.saveAsTextFiles("output/test1","")

		//fetcherService.map(_.contentType).print()
		fetcherService.map(x => (x.contentType, 1)).reduceByKey(_ + _).print()
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