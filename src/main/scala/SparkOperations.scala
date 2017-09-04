import java.io.{File, PrintWriter}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object SparkOperations {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark Tutorial")

    val sparkContext = new SparkContext(sparkConf)

    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    //--------Question 1. Create RDD------------------------------

    val pagecounts: RDD[String] = sparkSession.sparkContext.textFile("src/main/resources/pagecounts-20151201-220000")

    //---------Question 3. Total records in dataset - Answer = 7598006----------------------------

    println(pagecounts.count())

    //------------​Question 4. RDD​ ​ containing only​ ​ English​ ​ pages-----------------------------

    val pagecountsWithEn: RDD[String] = pagecounts.filter(pageCount => pageCount.split(" ")(0).equals("en"))

    println(pagecountsWithEn.collect().toList)

    //----------------Question 5. Number of records of english pages - Answer = 2278417--------------------

    println(pagecountsWithEn.count())

    //---------------Question 6. pages​ ​ that​ ​ were​ ​ requested​ ​ more​ ​ than​ ​ 200,000​ ​ times-------------------

    val highRequestPageCount = pagecounts.map{pageCount =>
      val arrayOfWords = pageCount.split(" ")
      (arrayOfWords(1), arrayOfWords(2).toInt)
    }
      .reduceByKey(_ + _)
      .filter(requestCount => requestCount._2 > 200000).collect()

    println(highRequestPageCount.toList) //11

    //------------Question 2. Get 10 records and write to a file------------------------

    val tenRecords = pagecounts.take(10).toList

    val writer = new PrintWriter(new File("src/main/resources/output.txt"))

    writer.write(tenRecords.mkString("\n"))

    writer.close()
  }
}
