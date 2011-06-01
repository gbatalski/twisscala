/**
 *
 */
package de.batalski.twisscala.model

import org.apache.cassandra.thrift.ConsistencyLevel._
import de.batalski.twisscala.lib.cassandra.client.CassandraService

/**
 * @author gbatalski
 *
 */

object TwisscalaUtils extends TwisscalaUtils{
    
}

class TwisscalaUtils {
  // ConsLevels
  val WRITE_CL = ONE;

  val READ_CL = ONE;

  // Column Family names
  val USERS = "User"

  val FRIENDS = "Friends"

  val FOLLOWERS = "Followers"

  val TWEETS = "Tweet"

  val TIMELINE = "Timeline"

  val USERLINE = "Userline"

  val cassService = CassandraService.apply


  
  def createModel = {
    cassService createColumnFamilyIfAbsent USERS
    cassService createColumnFamilyIfAbsent TWEETS
    cassService createColumnFamilyIfAbsent FOLLOWERS
    cassService createColumnFamilyIfAbsent FRIENDS
    cassService createColumnFamilyIfAbsent TIMELINE
    cassService createColumnFamilyIfAbsent USERLINE
  }

  def getTweet(tweetid: String): Tweet = {
    val cols = cassService.listColumns(tweetid, TWEETS)
    new Tweet(tweetid, cols.get("uname").get, cols.get("body").get)
  }

  // Data Reading
  def getUserByUsername(uname: String): User = {

    val password = cassService.readColumn(uname, "password", USERS)

    if (null == password || password.equals("")) {
      return null
    }

    new User(uname, password)
  }

  def getFriendUnames(uname: String): List[String] = {
    getFriendUnames(uname, 5000)
  }

  def getFriendUnames(uname: String, count: Int): List[String] = {
    getFriendOrFollowerUnames(FRIENDS, uname, count)
  }

  def getFollowerUnames(uname: String): List[String] = {
    getFollowerUnames(uname, 5000)
  }

  def getFollowerUnames(uname: String, count: Int): List[String] = {
    getFriendOrFollowerUnames(FOLLOWERS, uname, count)
  }

  def getFriends(uname: String): List[User] = {
    getFriends(uname, 5000)
  }

  def getFriends(uname: String, count: Int): List[User] = {
    def friendUnames = getFriendUnames(uname, count)
    getUsersForUnames(friendUnames)
  }

  def getFollowers(uname: String): List[User] = {
    getFollowers(uname, 5000)
  }

  def getFollowers(uname: String, count: Int): List[User] = {
    def followerUnames = getFollowerUnames(uname, count)
    getUsersForUnames(followerUnames)
  }

  def getTimeline(uname: String): Timeline = {
    getTimeline(uname, "", 40)
  }

  def getTimeline(uname: String, startkey: Long): Timeline = {
    var longAsStr = ""
    if (startkey.asInstanceOf[java.lang.Long]!=null)
      longAsStr = String.valueOf(startkey)

    getTimeline(uname, longAsStr, 40)
  }

  def getTimeline(uname: String, startkey: String, limit: Int): Timeline = {
    getLine(TIMELINE, uname, startkey, limit)
  }

  def getUserline(uname: String): Timeline = {
    getUserline(uname, "", 40)
  }

  def getUserline(uname: String, startkey: Long): Timeline = {
    var longAsStr = ""
    if (startkey.asInstanceOf[java.lang.Long]!=null)
      longAsStr = startkey.toString

    getUserline(uname, longAsStr, 40)
  }

  def getUserline(uname: String, startkey: String, limit: Int): Timeline = {
    getLine(USERLINE, uname, startkey, limit)
  }
  // Helpers
  private def getFriendOrFollowerUnames(cf: String, uname: String, count: Int): List[String] = {

    cassService.listColumns(uname, cf, null, count).keySet.toList

  }

  private def getLine(cf: String, uname: String, startkey: String, count: Int): Timeline = {

    val map = cassService.listColumns(uname, cf, startkey, count)

    if (null == map || 0 == map.size) {
      return null
    }

    val tweetids = map.values

    val tweets = getTweetsForTweetids(tweetids.toList)
    val keySet = map.keySet
    
    new Timeline(tweets, 0)

  }

  def getTweetsForTweetids(tweetids: List[String]): List[Tweet] = {

    val tweets = List[Tweet]()

    for (tweetid <- tweetids) {
      tweets.+:(getTweet(tweetid))          
    }

   tweets
  }
  def getUsersForUnames(unames: List[String]): List[User] = {

    val data = cassService.readColumns(unames.toArray, Array[String]("uname", "password"), USERS)

    val users = List[User]()

    for ((rowKey, columns) <- data) {
      users.+:(new User(rowKey, columns.get("password").get))
    }
    users
  }
  
  
  
  
	// Data Writing
   def saveUser(user:User) {
		cassService.updateColumn(user.getUsername,
								user.getPassword,
								"password",
								USERS);
	}

}