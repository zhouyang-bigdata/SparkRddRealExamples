/**
  * @ClassName NAStatCounter
  * @Description TODO
  * @Author zy
  * @Date 2019/6/15 11:19
  * @Version 1.0
  **/
package com.app.datacleaning

import org.apache.spark.util.StatCounter

/*
 * @Author zhouyang
 * @Description //TODO 主要统计记录数据缺失情况下的均值、方差、最小值、最大值
 * @Date 11:19 2019/6/15
 * @Param
 * @return
 **/

class NAStatCounter extends Serializable {
  val stats: StatCounter = new StatCounter()
  var missing: Long = 0

  def add(x: Double): NAStatCounter = {
    if (x.isNaN) {
      missing += 1
    } else {
      stats.merge(x)
    }
    this
  }

  def merge(other: NAStatCounter): NAStatCounter = {
    stats.merge(other.stats)
    missing += other.missing
    this
  }

  override def toString: String = {
    "stats: " + stats.toString + " NaN: " + missing
  }
}

object NAStatCounter {
  def apply(x: Double) = (new NAStatCounter).add(x)
}
