package com.example

import org.apache.spark._

object Hello {
  def main(args: Array[String]): Unit = {
    println("Hello, world!")
    val sparkConf = new SparkConf().setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    val one_step_wordcount = sc.textFile("hdfs:///user/cloudera/ratings.csv").flatMap(line=>line.split(" ")).map(line=>(line,1)).reduceByKey(_+_).saveAsTextFile("hdfs:///user/cloudera/output_wordcount_2")
    println("Done!")
  }
}
