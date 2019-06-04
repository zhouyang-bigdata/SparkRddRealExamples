/**
  * @ClassName WordCountByReduceByKey
  * @Description TODO
  * @Author zy
  * @Date 2019/6/4 14:56
  * @Version 1.0
  **/
package com.app.wordcount

import org.apache.spark.{SparkConf, SparkContext}

object WordCountByReduceByKey {
  def main(args: Array[String]): Unit = {
    // 非常重要，是通向Spark集群的入口
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val sales = sc.parallelize( Seq( ("idA", 2), ("idA", 4), ("idB", 3), ("idB",5),("idB",10),("idC",7) ) )
    val counts = sales.mapValues((1,_))
      .reduceByKey {case (a,b) => ((a._1+b._1),(a._2+b._2))}

    counts.collect.foreach(println)
  }
}
