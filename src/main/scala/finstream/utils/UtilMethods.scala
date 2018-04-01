package finstream.utils

/*
 * Collection of utility methods used in various parts of the application.
 * @author: arvind-rs
 * @date: 04/01/2018
 */

object UtilMethods {

	// Method to test if a given URL is a valid URL or not
	def isValidUrl(url: String): Boolean = {
		if(url.startsWith("http://") || url.startsWith("https://"))
			return true
		return false
	}
}