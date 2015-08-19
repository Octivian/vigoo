package com.dsp.statistics

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/**
 * Created by tianpo on 2015/8/12.
 */

object UserStatistics {

  val ALL = "AL"

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: <file>")
      System.exit(1)
    }


    case class User(dates: String, logType: String, ip: String, ua: String, uv: String, clientSize: String, mediaUuid: String, url: String,
                    host: String, adInfoUuid: String)

    val conf = new SparkConf()

    val sc = new SparkContext(conf)

    val lines = sc.textFile(args(0))

    val rdd1 = lines.map(line => {
      val r = line.split("\t")
      User(r(0), r(1), r(2), r(3), r(4), r(5), r(6), r(7), r(8), r(9))
    })


    val seqOp = (a: (Int, List[String]), b: User) => a match {
      case (0, List()) => (1, List(b.url))
      case _ => (a._1 + 1, b.url :: a._2)
    }

    val combOp = (a: (Int, List[String]), b: (Int, List[String])) => {
      (a._1 + b._1, a._2 ::: b._2)
    }


    val rdd2 = rdd1.map(r => ((r.adInfoUuid, ALL, 10), r))
    rdd2.aggregateByKey((0, List[String]()))(seqOp, combOp).map(a => {
      (a._1, a._2._1, a._2._2.distinct.length)
    })

    //line.flatMap(_.split("\t")).map((_,1)).reduceByKey(_+_).collect.foreach(println)

    lines.filter(filterLog(_)).map(buildKeyValuePair(_)).countApproxDistinctByKey()

    //lines.map(buildKeyValuePairTest(_)).aggregateByKey((0, List[String]()))(_,_)

    sc.stop

  }

  def buildKeyValuePairTest(line: String): (String, String) = {
    val keyValuePair = (line.split("\t")(0),
      line.split("\t")(1) + "\t" + line.split("\t")(2) + "\t" + line.split("\t")(3))
    return keyValuePair
  }

  def buildKeyValuePair(line: String): (String, String) = {
    val keyValuePair = (ALL + "\t" + line.split("\t")(0).split(" ")(0) + "\t" + line.split("\t")(6) + "\t" + 20,
      line.split("\t")(1) + "\t" + line.split("\t")(2).split(" ")(0) + "\t" + line.split("\t")(4))
    return keyValuePair
  }

  def filterLog(logEvent: String): Boolean = {
    val split = logEvent.split("\t")
    val action = split(1)
    return (split.length > 9 && !split(9).trim.equals("null")) && ("91".equals(action) || "92".equals(action) || "21".equals(action) || "22".equals(action) || "51".equals(action) || "305".equals(action))
  }

  def test(value1: String, value2: String): String = {
    val action1 = value1.split("\t")(0)
    val ip1 = value1.split("\t")(1)
    val uv1 = value1.split("\t")(2)
    val action2 = value2.split("\t")(0)
    val ip2 = value2.split("\t")(1)
    val uv2 = value2.split("\t")(2)
    if (action1.equals(action2)) {
      if ("91".equals(action1)) {

      }
      if ("92".equals(action1)) {

      }
      if ("51".equals(action1) || "305".equals(action1)) {

      }
      if ("21".equals(action1) || "22".equals(action1) || "305".equals(action1)) {

      }
    }
    return action1
  }

}
