/**
  * @ClassName WordCountByGroupByKey
  * @Description TODO
  * @Author zy
  * @Date 2019/6/4 14:47
  * @Version 1.0
  **/
package com.app.wordcount

import org.apache.spark.{SparkConf, SparkContext}

object WordCountByGroupByKey {
  def main(args: Array[String]): Unit = {
    // 非常重要，是通向Spark集群的入口
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val sales = sc.parallelize( Seq( ("idA", 2), ("idA", 4), ("idB", 3), ("idB",5),("idB",10),("idC",7) ) )
    val counts = sales.groupByKey()
      .mapValues(sq => (sq.size,sq.sum))

    counts.collect.foreach(println)
  }

}
