
package com.app.datacleaning

import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
/**
  * @ClassName StatsWithMissing
  * @Description TODO
  * @Author zy
  * @Date 2019/6/15 11:20
  * @Version 1.0
  **/
package object StatsWithMissing {
  /**
    * Double数组数据统计
    * @param rdd
    * @return
    */
  def statsWithMissing(rdd: RDD[Array[Double]]): Array[NAStatCounter] = {
    val nastats = rdd.mapPartitions((iter: Iterator[Array[Double]]) => {
      var nasB: ArrayBuffer[NAStatCounter] = new ArrayBuffer[NAStatCounter](10)
      while(iter.hasNext){
        val nas = iter.next().map(d => NAStatCounter(d))
        iter.foreach(arr => {
          nas.zip(arr).foreach { case (n, d) => n.add(d) }
        })
        for(i<-0 until nas.length){
          nasB.append(nas.apply(i))
        }
      }


      Iterator(nasB.toArray)

    })
    nastats.reduce((n1, n2) => {
      n1.zip(n2).map { case (a, b) => a.merge(b) }
    })
  }
}
