package com.bravolt.boss2016

import java.net.URI
import java.time.temporal.TemporalField

import com.fasterxml.jackson.core.JsonParser.Feature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

/**
  * Created by dclifford on 11/18/16.
  */
case class Vote(choice: String, location: String, time: String) extends Serializable

object ResultsApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Results")
    val ctx = new SparkContext(conf)


    val votes = ctx.textFile("hdfs://localhost:54310/votes/*").flatMap(input => {
      val mapper = new ObjectMapper()
      mapper.registerModule(DefaultScalaModule)
      mapper.configure(Feature.ALLOW_SINGLE_QUOTES, true)

        try {
          Some(mapper.readValue(input, classOf[Vote]))
        } catch {
          case e: Exception => None
        }
    })

    val totalVotesForRDD = (votes.filter(_.choice == "yes"))
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

    println(s"for: $totalVotesFor, against: $totalVotesAgainst, total: $totalVotes")
    println(s"fraud alert: $fraudulantVotes appear to have voted fraudulantly")
    println(s"bad ballots: $badBallots")

    //val votesByLocation = jsons.map(input => (input.asInstanceOf[Votes].location, 1)).reduceByKey((a,b) => a + b).collect.foreach(println)*/

  }
}
