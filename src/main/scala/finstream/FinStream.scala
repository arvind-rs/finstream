
/*
 * Data pipeline to collect, transform and store financial price and social media text data
 * for a given list of stocks using Spark streaming.
 * @author: arvind-rs
 * @date: 03/30/2018
 */

/*
 * Things to do:
 * 7. Add application level logs
 * 8. Add Configuration files
 */

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._ 
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.rdd.JdbcRDD
import java.util.Properties
import java.sql.{Connection, DriverManager, ResultSet}

import finstream.receivers._
import finstream.scraper._
import finstream.utils._


object FinStream {

	val intervalWindowMinutes = 1
	val masterConfig = "conf/master.conf"
	val scrapeConfigPath = UtilMethods.getMasterConfig.getString("SCRAPE_CONFIG")
	val scrapeConfig = UtilMethods.loadScrapeConfig(scrapeConfigPath)(0)

	val urlList = List("https://www.google.com/search?q=FB", "https://www.google.com/search?q=AMZN","https://www.google.com/search?q=GOOG")
	//val urlList = List("https://www.google.com/search?q=FB")

	val connectionUrl = "jdbc:mysql://localhost:3306/finstream"
	val username = ""
	val password = ""
	val stockPriceTable = "stock_price"

	case class StockPrice(ticker: String, marketCap: Double, price: Double, exchange: String, ltt: String)

	def main(args: Array[String]) {

		// Set logging level
		Logger.getLogger("org").setLevel(Level.ERROR)

		// Set the configurations and get the required contexts
		val sparkConf = new SparkConf().setMaster("local[4]").setAppName("Test")
		val sc = new SparkContext(sparkConf) 
		val sqlContext = new SQLContext(sc)
		import sqlContext.implicits._
		val ssc = new StreamingContext(sc, Seconds(intervalWindowMinutes * 60))

		// Listen to the receiver
		val fetcherService = ssc.receiverStream(new Fetcher(urlList))

		// Scrape the relevant data
		val htmlScrapeDataDStream = fetcherService.map(rdd => Scraper.scrapeHTML(rdd.response, scrapeConfig))
		
		// Transform the Scraper output into structured data using a case class
		val stockPriceDataDStream = htmlScrapeDataDStream.map(_.htmlScrapeDataList).map { rdd =>
			try {
				val dataPointList = List("ticker","market_cap","price","exchange","ltt")
				val list = scala.collection.mutable.ListBuffer[String]()
				for(dataPoint <- dataPointList) {
					val htmlScrapeData = rdd.filter(_.dataPoint.equals(dataPoint))
					val temp = if(htmlScrapeData.size > 0) htmlScrapeData(0).data else ""
					list += temp
				}
				StockPrice(list(0), list(1).replaceAll(",","").toDouble, list(2).replaceAll(",","").toDouble, list(3), list(4))
			} 
			catch {
				case ex: RuntimeException => println(ex)
			}
		}
		
		// Write the structured data into a DB table
		Class.forName("com.mysql.jdbc.Driver").newInstance
		stockPriceDataDStream.foreachRDD { rdd =>
			rdd.foreachPartition { iterator =>
				val conn = DriverManager.getConnection(connectionUrl,username,password)
				val statement = conn.prepareStatement(s"INSERT INTO ${stockPriceTable}(ticker, market_cap, price, exchange, ltt) values(?,?,?,?,?);")
				for(data <- iterator) {
					try {
						val stockPrice = data.asInstanceOf[StockPrice]
						statement.setString(1, stockPrice.ticker)
						statement.setDouble(2, stockPrice.marketCap)
						statement.setDouble(3, stockPrice.price)
						statement.setString(4, stockPrice.exchange)
						statement.setString(5, stockPrice.ltt)
						statement.executeUpdate
					} catch {
						case ex: RuntimeException => println(ex)
					}
				}
				conn.close()
			}
		}

		// Start the Spark application
		ssc.start()
		ssc.awaitTermination()
	}
}