package com.accu.TestAccu

import org.apache.spark.util.AccumulatorV2


class Customer extends AccumulatorV2[Int,Int]{

  //定义一个属性
  var sum = 0

  //判断是否空
  override def isZero: Boolean = sum==0

  //复制一个累加器
  override def copy(): AccumulatorV2[Int, Int] = {
    val customer = new Customer

    customer.sum=this.sum

    customer
  }


  //重置累加器
  override def reset(): Unit = sum=0

  //重置累加器
  override def add(v: Int): Unit = sum+=v

  //合并结果
  override def merge(other: AccumulatorV2[Int, Int]): Unit = {

    this.sum+=other.value


  }


    //得到返回结果
  override def value: Int = sum


}
