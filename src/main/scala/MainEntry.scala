import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.io.Source

object MainEntry {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("myapp").master("local[*]").getOrCreate()
    val USER_FILE_PATH = "src/resource/users.dat"
    val MOVIE_FILE_PATH = "src/resource/movies.dat"
    val RATING_FILE_PATH = "src/resource/ratings.dat"

    //read as text
    val users = spark.sparkContext.textFile(USER_FILE_PATH)
    val movies = spark.sparkContext.textFile(MOVIE_FILE_PATH)
    val ratings = spark.sparkContext.textFile(RATING_FILE_PATH)

    //map to case class
    val rddUsers = users.map(mapToUsers)
    val rddMovies = movies.map(mapToMovies)
    val rddRatings = movies.map(mapToRatings)

    //convert rdd to dataframe
    val dfUserWithSchema = spark.createDataFrame(rddUsers).toDF("id", "gender","age","occupation","zipcode")
    val dfMoviesWithSchema = spark.createDataFrame(rddMovies).toDF("movieId", "title","genre")
    val dfRatingsWithSchema = spark.createDataFrame(rddRatings).toDF("userId", "movieId","rating","timestamp")


  }
  def mapToUsers(line : String) : User = {
    var splitted = line.split("::")
    User(splitted(0),splitted(1),splitted(2).toInt,splitted(3),splitted(4))
  }
  def mapToMovies(line : String) : Movie = {
    var splitted = line.split("::")
    Movie(splitted(0),splitted(1),splitted(2))
  }
  def mapToRatings(line : String) : Rating = {
    var splitted = line.split("::")
    Rating(splitted(0),splitted(1),splitted(2).toDouble,splitted(3))
  }
  case class User(id : String,gender : String,age:Int,occupation:String,zipCode : String)
  case class Movie(movieID : String,title : String,genre : String)
  case class Rating(userID: String,movieID:String,rating: Double,timestamp: String)

}
