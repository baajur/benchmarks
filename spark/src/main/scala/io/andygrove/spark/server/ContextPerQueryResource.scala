package io.andygrove.spark.server

import javax.ws.rs.core.MediaType
import javax.ws.rs.{GET, POST, Path, Produces, QueryParam}
import org.apache.spark.sql.SparkSession

import scala.collection.concurrent.TrieMap

@Path("/context-per-query")
@Produces(Array(MediaType.APPLICATION_JSON))
class ContextPerQueryResource(conf: MyConfiguration) {

  /** Map of table name to path */
  val tables = TrieMap[String, String]()

  @POST
  @Path("/register")
  def registerTable(@QueryParam("tableName") tableName: String, @QueryParam("path") path: String): Unit = {
    tables.put(tableName, path)
  }

  @GET
  def query(@QueryParam("sql") sql: String): String = {

    val spark: SparkSession = SparkSession.builder
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    for ((k,v) <- tables) {
      SparkUtils.loadParquet(spark, k, v)
    }

    SparkUtils.createResponse(spark.sql(sql))
  }

}
