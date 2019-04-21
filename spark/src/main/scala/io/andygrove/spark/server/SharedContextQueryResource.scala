package io.andygrove.spark.server

import javax.ws.rs.core.MediaType
import javax.ws.rs.{GET, POST, Path, Produces, QueryParam}
import org.apache.spark.sql.SparkSession

@Path("/shared-context")
@Produces(Array(MediaType.APPLICATION_JSON))
class SharedContextQueryResource(conf: MyConfiguration) {

  val spark: SparkSession = SparkSession.builder
    .appName(this.getClass.getName)
    .master("local[*]")
    .getOrCreate()

  @POST
  @Path("/register")
  def registerTable(@QueryParam("tableName") tableName: String, @QueryParam("path") path: String): Unit = {
    SparkUtils.loadParquet(spark, tableName, path)
  }

  @GET
  def query(@QueryParam("sql") sql: String): String = {
    SparkUtils.createResponse(spark.sql(sql))
  }

}
