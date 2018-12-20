package com.mpojeda84.mapr.scala

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.ojai.store._

import scala.collection.JavaConverters._
import Json._

case class Business(_id: String, address: String, open: Boolean)

object Json {

  implicit class Serializer[A](a: A) {

    private lazy val objectMapper = new ObjectMapper()
    objectMapper.registerModule(DefaultScalaModule)

    def toJSON() = objectMapper.writeValueAsString(a)

  }

}

object app {

  val connection = DriverManager.getConnection("ojai:mapr:")
  println("Connection opened")

  def closeConnection(): Unit = connection.close()

  def insertBusinesses(): Unit = {
    val store = connection.getStore("/user/mapr/tables/business")

    val count = (1 to 5)
    count.foreach(x => {
      val business = Business(x.toString, x + " Street, Fort Lauderdale", x % 2 == 0)
      val document = connection.newDocument(business.toJSON())
      store.insertOrReplace(document)
    })
    store.close()
    println("Inserted/Updated 5 Documents")
  }

  def printBusiness2(): Unit = {
    val store = connection.getStore("/user/mapr/tables/business")
    val document = store.findById("2")
    println("Business with id: 2 is --> " + document.asJsonString())
    store.close()
  }

  def printAllBusinesses(): Unit = {
    val store = connection.getStore("/user/mapr/tables/business")
    println("All Businesses in table")
    store.find().iterator().asScala.foreach(println)
    store.close()
  }

  def printOpenBusinesses(): Unit = {
    val store = connection.getStore("/user/mapr/tables/business")
    println("All Open Businesses in table")

    val query = connection.newQuery.where(connection.newCondition.is("open", QueryCondition.Op.EQUAL, true).build).build
    val stream = store.find(query)
    stream.forEach(x => println(x.asJsonString))

    store.close()
  }

  private def printClosedBusinesses(): Unit = {
    println("All Closed Business in table")
    val store = connection.getStore("/user/mapr/tables/business")
    val query = connection.newQuery.where("{\"$eq\": {\"open\": false}}").build
    val stream = store.find(query)
    stream.forEach((x) => println(x.asJsonString))
    store.close()
  }

  def main(args: Array[String]): Unit = {

    insertBusinesses()
    println("------------------------")

    printBusiness2()
    println("------------------------")

    printAllBusinesses()
    println("------------------------")

    printOpenBusinesses()
    println("------------------------")

    printClosedBusinesses()
    println("------------------------")

    closeConnection()
  }
}