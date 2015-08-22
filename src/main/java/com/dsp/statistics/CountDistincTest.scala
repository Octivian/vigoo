package com.dsp.statistics

/**
 * Created by SecondTheWorld on 15/8/22.
 */

import org.apache.spark.{SparkContext, SparkConf}

object CountDistincTest {
  def main(args: Array[String]) {

    val conf = new SparkConf()

    val sc = new SparkContext(conf)

    val lines = sc.textFile(args(0))

    val rdd1 = lines.filter(filterDatas).map(line=>(line.split("\t")(11),line))

    /**
     * worked count distinct
     */
    rdd1.combineByKey(
      (v: String) => {
        List(v.split("\t")(2))
      },
      (c: List[String],v: String) => {
        v.split("\t")(2)::c
      },
      (c1: List[String],c2: List[String])=>{
        c1:::c2
      }
    ).map(kv=>(kv._1,kv._2.distinct.length)).saveAsTextFile(args(1))

  }

  private def filterDatas(lines: String): Boolean = {
    lines.split("\t")(1).equals("21")||lines.split("\t")(1).equals("22")
  }
}
