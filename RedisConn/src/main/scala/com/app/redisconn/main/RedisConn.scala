/**
  * @ClassName RedisConn
  * @Description TODO
  * @Author zy
  * @Date 2019/6/1 18:01
  * @Version 1.0
  **/
package com.app.redisconn.main

import com.app.redisconn.utils.RedisUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe


object RedisConn {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("RedisConn")
    val streamingContext = new StreamingContext(conf, Seconds(1))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("topicA", "topicB")
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream.foreachRDD(rdd => {
      rdd.foreachPartition(it => {
        var exceptionExist = false
        val jedis = RedisUtil.getConnection
        it.foreach(record => {
          try{
            //
            //record 根据kafka生产者生成的实体类而定
//            if(jedis != null && jedis.exists("abc" + record._2 + record._3)){
//              //从Redis中获取数据
//              val value = jedis.hmget("abc" + record._2 + record._3,"a1","a2","a3","a4","a5","a6")
//              //使用数据。。。
//            }
          }catch{
            case e: Exception => {
              println(e)
              exceptionExist = true
            }
          }
        })
        RedisUtil.closeConnection(jedis,exceptionExist)
        exceptionExist = false
      })
    })

  }
}
