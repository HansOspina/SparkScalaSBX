package com.hansospina.spark

import java.net.URI

import com.sun.scenario.Settings
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.commons.io.IOUtils
import org.apache.http.HttpHeaders
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.impl.client.HttpClients
import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import org.json4s._
import org.json4s.jackson.JsonMethods._


object SBXQuery {


  sealed trait Data

  case class Customer(companyName: String)

  case class Response[T](success: Boolean, error:Option[String], results: List[T])


  def loadFromSBX(): String = {
    val client = HttpClients.createDefault()
    val uri = new URI(s"http://54.71.230.34/api/data/v1/row/find")
    val post = new HttpPost(uri)
    post.addHeader(HttpHeaders.ACCEPT, ContentType.APPLICATION_JSON.toString)
    post.addHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.toString)
    post.addHeader(HttpHeaders.AUTHORIZATION, s"Bearer ${scala.util.Properties.envOrElse("TOKEN", "DOOM")}")
    println(post.toString)

    post.setEntity(new StringEntity("{\"row_model\":\"customer\",\"domain\":\"129\",\"page\":1,\"size\":100,\"fetch\":[\"state\",\"country\",\"time_zone\",\"voucher\"]}"))

    val resp = client.execute(post)

    try {
      val entity = resp.getEntity
      IOUtils.toString(entity.getContent, StandardCharsets.UTF_8)
    } finally {
      resp.close()
    }
  }


  /** Our main function where the action happens */
  def main(args: Array[String]) {

    val jsonStr = parse(loadFromSBX())

    implicit val formats: DefaultFormats.type = DefaultFormats
    val coco = jsonStr.camelizeKeys.extract[Response[Customer]]


    if (coco.success) {
      coco.results.foreach(println)
    } else {
      println(coco.error.getOrElse("No error but didn't work dude."))
    }



    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "LPS Logs")
    val spark = SparkSession
      .builder()
      .getOrCreate()
    import spark.implicits._


    sc.stop()


  }

}
