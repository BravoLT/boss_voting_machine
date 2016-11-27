package com.bravolt.boss2016

import java.io.{BufferedReader, InputStreamReader}
import java.net.URI

import com.fasterxml.jackson.core.JsonParser.Feature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.io.Source

//case class Vote(choice: String, location: String, time: String) extends Serializable

object BetterResults {

  def main(args : Array[String]) : Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Results")
    val ctx = new SparkContext(conf)
    val fs = FileSystem.get(new URI("hdfs://localhost:54310"), ctx.hadoopConfiguration)
    val status = fs.listStatus(new Path("/votes"))
    val files = status.map(input => input.getPath().getName)

    val votes = ctx.parallelize(files).flatMap(input => {
      val clientFs = FileSystem.get(new URI("hdfs://localhost:54310"), new Configuration())
      val path = input.toString.replace("hdfs://localhost:54310", "")
      val file = clientFs.open(new Path(s"/votes/$path"))
      val reader = new BufferedReader(new InputStreamReader(file))
      val mapper = new ObjectMapper()

      mapper.registerModule(DefaultScalaModule)
      mapper.configure(Feature.ALLOW_SINGLE_QUOTES, true)

      var results = List[Vote]()
      Stream.continually(reader.readLine()).takeWhile(_ != null).foreach(input => {
        try {
          results = mapper.readValue(input, classOf[Vote]) :: results
        }  catch {
          case e: Exception => {
            println(s"failed to parse $input")
          }
        }

      })
      results
    }).cache()

    val totalVotesForRDD = votes.filter(_.choice == "yes")
    val totalVotesAgainstRDD = (votes.filter(_.choice == "no"))
    val badBallotsRDD = votes.filter(_.choice != "yes").filter(_.choice != "no")
    val yesVotesByLocationRDD = votes.filter(_.choice == "yes").map(input => (input.location, 1));
    val noVotesByLocationRDD = votes.filter(_.choice == "no").map(input => (input.location, 1));
    val fraudulantVotesByLocationRDD = votes.map(input => {
      val voteTime = java.time.LocalDateTime.ofInstant(java.time.Instant.ofEpochMilli(input.time.toLong), java.time.ZoneId.systemDefault())

      if(voteTime.getHour > 20 || voteTime.getHour < 7) {
        (input.location, 1)
      } else {
        (None, None)
      }
    }).filter(_._1  != None)

    val totalVotes = votes.count()
    val totalVotesFor = totalVotesForRDD.count()
    val totalVotesAgainst = totalVotesAgainstRDD.count()
    val yesVotesByLocation = yesVotesByLocationRDD.countByKey()
    val noVotesByLocation = noVotesByLocationRDD.countByKey()
    val fraudulantVotesByLocation = fraudulantVotesByLocationRDD.countByKey()
    val fraudulantVotes = fraudulantVotesByLocationRDD.count()
    val badBallots = badBallotsRDD.count()

    println(s"totalVotes: $totalVotes")
    println(s"for: $totalVotesFor, against: $totalVotesAgainst, totalVotes: $totalVotes")
    println(s"fraud alert: $fraudulantVotes appear to have voted fraudulantly")
    println(s"bad ballots: $badBallots")

    //files.foreach(println)
  }

}
