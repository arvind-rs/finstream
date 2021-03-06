package finstream.receivers

/*
 * A custom receiver to fetch a list of static web pages or JSON responses from APIs.
 * @author: arvind-rs
 * @date: 04/01/2018
 */

import java.io._
import java.net._
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import finstream.utils._

class Fetcher(urlList: List[String]) extends Receiver[FetcherResponse](StorageLevel.MEMORY_AND_DISK_2) with Logging {

	val intervalWindowMinutes = 1
	var stopThread: Boolean = false
	// Overridden method to start the thread for fetching the pages
	def onStart() {
		val thread = new Thread("Fetcher") {
			override def run() { 
				while(!stopThread) {
					try {						
						println("Thread running")
						fetch()
						Thread.sleep(intervalWindowMinutes * 60 * 1000)
					} catch {
						case ex: Exception => {throw new RuntimeException(ex)}
					}
				}
			}
		}
		thread.start()
	}

	// Overridden method to stop receiving the data
	def onStop() {
		// Currently not doing anything.
		stopThread = true
		stop("Stop for this interval")
	}

	// Fetch the list of URLs and store the webpages in memory
	private def fetch() {
		var bufferedSource: scala.io.BufferedSource = null
		//while(!isStopped) {
		for(url <- urlList; if UtilMethods.isValidUrl(url)) {
			println("fetching " + url)
			try {
				// Establish the connection
				val connect = new URL(url)
				val connection = connect.openConnection
				connection.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.1")
				// Get the content type
				val contentType = connection.getContentType
				// Get the response
				bufferedSource = scala.io.Source.fromInputStream(connection.getInputStream)
				var response = bufferedSource.getLines.mkString
				if(contentType != null && response != null) {
					store(FetcherResponse(contentType, response))
				}
			} catch {
				case ex: RuntimeException => { 
					println(ex)
					restart(ex.getMessage) 
				}
			} finally {
				if(bufferedSource != null) bufferedSource.close()
			}
		}
	}
}