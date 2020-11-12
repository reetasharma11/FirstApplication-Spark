import java.nio.charset.CodingErrorAction

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.io.Codec
import scala.io.Source.fromFile

object Exercise2 {
  /** Load up a Map of movie IDs to movie names. */
  def loadMovieNames(): Map[Int, String] = {
      // Create a Map of Ints to Strings, and populate it from u.item.
      implicit val codec = Codec("UTF-8")
      codec.onMalformedInput(CodingErrorAction.REPLACE)
      codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
      var movieNames: Map[Int, String] = Map()

      val lines = fromFile("/home/reeta/IdeaProjects/untitled2/src/main/scala/ml-100k/u.item").getLines()
      for (line <- lines) {
        var fields = line.split('|')
        if (fields.length > 1) {
          movieNames += (fields(0).toInt -> fields(1))
        }
      }
      return movieNames
    }
    // Case class so we can get a column name for our movie ID
    final case class Movie(movieID: Int, rating:Int)

    def main(args: Array[String]) {
      Logger.getLogger("org").setLevel(Level.ERROR)

      val spark = SparkSession
        .builder
        .appName("AvgMoviesRatings")
        .master("local[*]")
        .getOrCreate()

      // Read in each rating line and extract the movie ID; construct an RDD of Movie objects.
      val lines = spark.sparkContext.textFile("/home/reeta/IdeaProjects/untitled2/src/main/scala/ml-100k/u.data").map(x => Movie(x.split("\t")(1).toInt,
        x.split("\t")(2).toInt))

      // Convert to a DataSet
      import spark.implicits._
      val moviesDS = lines.toDS()
      // Compute average rating for each movieID
      val averageRatings = moviesDS.groupBy("movieID").avg("rating")

      // Compute count of ratings for each movieID
      val counts = moviesDS.groupBy("movieID").count()
      //    counts.collect().foreach(println)

      // Join the two together (We now have movieID, avg(rating), and count columns)
      val popularAveragesAndCounts = counts.join(averageRatings, "movieID")

      // Create a broadcast variable of our ID -> movie name map
      var movieNames = loadMovieNames()
      //println(movieNames)

      //Pull the top 10 results
      val sortedRatings = popularAveragesAndCounts.orderBy("avg(rating)")

      //    for (movie <- sortedRatings){
      //      println(movieNames(movie(0).toString.toInt), movie(1), movie(2))
      //      println(movieNames(movie.getInt(0)), movie(1), movie(2))
      //    }

      sortedRatings.foreach(f => println(movieNames(f.getInt(0)), f(1), f(2)))
    }




}
