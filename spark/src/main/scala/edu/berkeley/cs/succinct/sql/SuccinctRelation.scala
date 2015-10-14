package edu.berkeley.cs.succinct.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.storage.StorageLevel

case class SuccinctRelation(
                             location: String,
                             userSchema: StructType = null)(@transient val sqlContext: SQLContext)
  extends BaseRelation with PrunedFilteredScan {

  val succinctTableRDD = SuccinctTableRDD(sqlContext.sparkContext, location, StorageLevel.MEMORY_ONLY).persist()
  private[succinct] var succinctSchema = getSchema

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    System.out.println("Building scan with cols:" + requiredColumns.mkString(",") + " and filters:"
      + filters.mkString(","))
    succinctTableRDD.pruneAndFilter(requiredColumns, filters)
  }

  private[succinct] def getSchema: StructType = {
    if (userSchema != null) {
      userSchema
    } else {
      val schemaPath = location.stripSuffix("/") + "/schema"
      val conf = sqlContext.sparkContext.hadoopConfiguration
      SuccinctUtils.readObjectFromFS[StructType](conf, schemaPath)
    }
  }

  private[succinct] def getAttributeIdx(attribute: String): Int = {
    succinctSchema.lastIndexOf(schema(attribute))
  }

  override def schema: StructType = succinctSchema

}
