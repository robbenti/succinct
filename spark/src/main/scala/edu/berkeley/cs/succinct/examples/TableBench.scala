package edu.berkeley.cs.succinct.examples

import java.io.FileWriter

import edu.berkeley.cs.succinct.SuccinctRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.Random

/**
 * Benchmarks search on a Wikipedia dataset provided as an input.
 */
object TableBench {

  // Constants
  val WARMUP_COUNT: Int = 20
  val MEASURE_COUNT: Int = 100

  // Metadata for executing Spark RDD queries
  var partitionOffsets: Seq[Long] = _
  var partitionSizes: Seq[Long] = _

  // Query data
  var searchQueries: Seq[(Int, String)] = _
  var recordIds: Seq[Long] = _
  var searchQueriesWarmup: Seq[(Int, String)] = _
  var searchQueriesMeasure: Seq[(Int, String)] = _
  var recordIdsWarmup: Seq[Long] = _
  var recordIdsMeasure: Seq[Long] = _

  // Output path
  var outPath: String = _

  def sampleSeq[T](input: Seq[T], sampleSize: Int): Seq[T] = {
    Seq.fill(sampleSize)(input(Random.nextInt(input.length)))
  }

  def searchRDD(rdd: RDD[(Long, Array[String])], colID: Int, value: String): RDD[Array[String]] = {
    rdd.filter(kv => kv._2(colID) == value).map(_._2)
  }

  def getRDD(rdd: RDD[(Long, Array[String])], recordId: Long): Array[String] = {
    val records = rdd.filter(kv => kv._1 == recordId).collect()
    if (records.length != 1) {
      throw new ArrayIndexOutOfBoundsException("Invalid recordId " + recordId)
    }
    return records(0)._2
  }

  def benchSparkRDD(rdd: RDD[(Long, Array[String])]): Unit = {
    val storageLevel = rdd.getStorageLevel match {
      case StorageLevel.DISK_ONLY => "disk"
      case StorageLevel.MEMORY_ONLY => "mem"
      case _ => "undf"
    }

    println(s"Benchmarking Spark RDD $storageLevel search recordIds...")

    // Warmup
    searchQueriesWarmup.foreach(q => {
      val count = searchRDD(rdd, q._1, q._2).count()
      println(s"$q\t$count")
    })

    // Measure
    val outSearch = new FileWriter(outPath + "/spark-" + storageLevel + "-search")
    searchQueriesMeasure.foreach(q => {
      val startTime = System.currentTimeMillis()
      val count = searchRDD(rdd, q._1, q._2).count()
      val endTime = System.currentTimeMillis()
      val totTime = endTime - startTime
      outSearch.write(s"$q\t$count\t$totTime\n")
    })
    outSearch.close()

    println(s"Benchmarking Spark RDD $storageLevel random access...")

    // Warmup
    recordIdsWarmup.foreach(r => {
      val length = getRDD(rdd, r).length
      println(s"$r\t$length")
    })

    // Measure
    val outExtract = new FileWriter(outPath + "/spark-" + storageLevel + "-extract")
    recordIdsMeasure.foreach(r => {
      val startTime = System.currentTimeMillis()
      val length = getRDD(rdd, r).length
      val endTime = System.currentTimeMillis()
      val totTime = endTime - startTime
      outExtract.write(s"$r\t$length\t$totTime\n")
    })
    outExtract.close()
  }

  def benchSuccinctRDD(rdd: SuccinctRDD): Unit = {
    println("Benchmarking Succinct RDD count recordIds...")

    // Warmup
    searchQueriesWarmup.foreach(q => {
      val count = rdd.search("|" + q._2 + "|").count()
      println(s"$q\t$count")
    })

    // Measure
    val outSearch = new FileWriter(outPath + "/succinct-search")
    searchQueriesMeasure.foreach(q => {
      val startTime = System.currentTimeMillis()
      val count = rdd.search("|" + q._2 + "|").count()
      val endTime = System.currentTimeMillis()
      val totTime = endTime - startTime
      outSearch.write(s"$q\t$count\t$totTime\n")
    })
    outSearch.close()

    println("Benchmarking Succinct RDD random access...")
    recordIdsWarmup.foreach(r => {
      val length = rdd.getRecord(r).length
      println(s"$r\t$length")
    })

    // Measure
    val outExtract = new FileWriter(outPath + "/succinct-extract")
    recordIdsMeasure.foreach(r => {
      val startTime = System.currentTimeMillis()
      val length = rdd.getRecord(r).length
      val endTime = System.currentTimeMillis()
      val totTime = endTime - startTime
      outExtract.write(s"$r\t$length\t$totTime\n")
    })
    outExtract.close()
  }

  def addRecordIds(it: Iterator[Array[String]], firstRecordId: Long): Iterator[(Long, Array[String])] = {
    var curRecordId = firstRecordId
    val buf: ArrayBuffer[(Long, Array[String])] = new ArrayBuffer[(Long, Array[String])]()
    while (it.hasNext) {
      val curRecord = (curRecordId, it.next())
      buf += curRecord
      curRecordId += 1L
    }
    buf.iterator
  }

  def main(args: Array[String]): Unit = {

    if (args.length < 5) {
      System.err.println("Usage: TableBench <raw-data> <succinct-data> <partitions> <query-path> <output-path>")
      System.exit(1)
    }

    val dataPath = args(0)
    val succinctDataPath = args(1)
    val partitions = args(2).toInt
    val queryPath = args(3)
    outPath = args(4)

    val sparkConf = new SparkConf().setAppName("TableBench")
    val ctx = new SparkContext(sparkConf)

    searchQueries = Source.fromFile(queryPath)
      .getLines().map(line => (line.split('\t')(0).toInt, line.split('\t')(1)))
      .toSeq

    // Create RDD
    val tableData = ctx.textFile(dataPath, partitions)
      .map(line => line.split('|'))
      .repartition(partitions)

    val partitionRecordCounts = tableData.mapPartitionsWithIndex((idx, partition) => {
      val partitionRecordCount = partition.size
      Iterator((idx, partitionRecordCount))
    }).collect.sorted.map(_._2)

    val partitionFirstRecordIds = partitionRecordCounts.scanLeft(0L)(_ + _)

    // Compute partition sizes and partition recordIds
    recordIds = Random.shuffle(partitionFirstRecordIds.zip(partitionRecordCounts)
      .map(range => (0 to 99).map(i => range._1 + (Math.abs(Random.nextLong()) % range._2)))
      .flatMap(_.iterator).toList)

    // Create queries
    searchQueriesWarmup = sampleSeq(searchQueries, WARMUP_COUNT)
    searchQueriesMeasure = sampleSeq(searchQueries, MEASURE_COUNT)
    recordIdsWarmup = sampleSeq(recordIds, WARMUP_COUNT)
    recordIdsMeasure = sampleSeq(recordIds, MEASURE_COUNT)

    val tableDataMem = tableData.mapPartitionsWithIndex((i, p) => {
      addRecordIds(p, partitionFirstRecordIds(i))
    }).cache()

    // Ensure all partitions are in memory
    println("Number of lines = " + tableDataMem.count())

    // Benchmark
    benchSparkRDD(tableDataMem)
    tableDataMem.unpersist()

    val tableSuccinctData = SuccinctRDD(ctx, succinctDataPath, StorageLevel.MEMORY_ONLY).persist()

    // Ensure all partitions are in memory
    println("Number of lines = " + tableSuccinctData.countOffsets("\n".getBytes()))

    // Benchmark Succinct
    benchSuccinctRDD(tableSuccinctData)
    tableSuccinctData.unpersist()

  }

}
