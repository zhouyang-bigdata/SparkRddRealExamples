/**
  * @ClassName WordCountMoreThan
  * @Description TODO
  * @Author zy
  * @Date 2019/6/4 15:30
  * @Version 1.0
  **/
package com.app.wordcount

import org.apache.spark.{SparkConf, SparkContext}

/*
 * @Author zhouyang
 * @Description //TODO 计算count大于某个值
 * @Date 15:31 2019/6/4
 * @Param
 * @return
 **/
object WordCountMoreThan {

  def main(args: Array[String]): Unit = {
    // 非常重要，是通向Spark集群的入口
    val conf = new SparkConf().setAppName("WordCountMoreThan").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val tokenized = sc.textFile("WordCount\\src\\main\\resources\\Abraham Lincoln.txt").flatMap(_.split(' '))
    val wordCounts = tokenized.map((_, 1)).reduceByKey(_ + _)
    val filtered = wordCounts.filter(_._2 >= 10)
    filtered.foreach(x=>println(x))
    val charCounts = filtered.flatMap(_._1.toCharArray).map((_, 1)).
      reduceByKey(_ + _)
    charCounts.collect().foreach(x=>println(x))
  }
}
