/**
  * @ClassName MaxSalary
  * @Description TODO
  * @Author zy
  * @Date 2019/6/4 14:26
  * @Version 1.0
  **/
package com.app.calmax

import org.apache.spark.{SparkConf, SparkContext}

/*
 * @Author zhouyang
 * @Description //TODO 求最大薪水。
 * @Date 14:43 2019/6/4
 * @Param 
 * @return  
 **/
object MaxSalary {

  def main(args: Array[String]): Unit = {
    // Setup configuration and create spark context
    val conf = new SparkConf().setAppName("MaxSalary").setMaster("local[2]")
    val sc = new SparkContext(conf)
    // Load the source data file into RDD
    val emp_data = sc.textFile("CalMax\\src\\main\\resources\\emp_data.txt")
    println(emp_data.foreach(println))
    // Find first record of the file
    val emp_header = emp_data.first()
    println(emp_header)
    // Remove header from RDD
    val emp_data_without_header = emp_data.filter(line => !line.equals(emp_header))
    println(emp_data_without_header.foreach(println))
    // Get no. of partition of an RDD
    println("No. of partition = " + emp_data_without_header.partitions.size)
    // Find max salary of an employe 1st Other approach
    // Using max function on RDD
    val emp_salary_list = emp_data_without_header.map{x => x.split(',')}.map{x => (x(5).toDouble)}
    println("Highest salaty:"+ emp_salary_list.max())
    // Using sortBy function
    val max_salary = emp_salary_list.distinct.sortBy(x => x.toDouble, false, 1)
    print(max_salary.take(1).foreach(println))
    
    // Find Second highest salary
    val second_highest_salary = max_salary.zipWithIndex().filter(index => index._2 == 1).map(_._1)
    print(second_highest_salary.foreach(println))
    
    // Employee who have max salary
    val salaryWithEmployeeName = emp_data_without_header.map{x => x.split(',')}.map{x => (x(5).toDouble, x(1))}
    val maxSalaryEmployee = salaryWithEmployeeName.groupByKey.takeOrdered(1)(Ordering[Double].reverse.on(_._1))
    print(maxSalaryEmployee.foreach(println))
  }
}
