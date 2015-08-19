package com.dsp.statistics

import com.mongodb.DBObject
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import scala.util.parsing.json.JSON

/**
 * Created by lixiang on 2015/8/19.
 */
object ResolveRules1 {

  def main(args: Array[String]) {
    val conf = new SparkConf()

    val sc = new SparkContext(conf)

    val lines = sc.textFile(args(0))

    val rules = MongoDBTest.getMongoDBRules()

    rules.foreach(rule => {
      computeDatas(lines, _).saveAsTextFile(args(1) + rule.get("biz_code").toString)
    })


  }

  case class Log(dates: String, logType: String, ip: String, os: String, model: String, browser: String, uv: String, clientSize: String, mediaUuid: String, url: String,
                 host: String, adInfoUuid: String)

  private def computeDatas(lines: RDD[String], rule: DBObject): RDD[(String, String)] = {
    val filtedRdd = lines.filter(filterDatas(_, rule))
    val rdd1 = filtedRdd.map(line => {
      line.split("\t").toList
    })
    resolveRule(rdd1, rule)
  }

  private def resolveRule(lines: RDD[List[String]], rule: DBObject): RDD[(String, String)] = {
    val groupColumnIndex = Integer.parseInt(rule.get("group_columns").toString)

    val json: Option[Any] = JSON.parseFull(rule.toString)
    val map: Map[String, Any] = json.get.asInstanceOf[Map[String, Any]]
    val kpiContent: List[Any] = map.get("kpi_content").get.asInstanceOf[List[Any]]
    val kpiContentArray: List[((Int, String, Int), List[String])] =
      for {kpiContentMap <- kpiContent
           countConditionColumn: Int = Integer.parseInt(kpiContentMap.asInstanceOf[Map[String, Any]].get("count_condition_column").get.asInstanceOf[String])
           countConditionColumnValue: String = kpiContentMap.asInstanceOf[Map[String, Any]].get("count_condition_column_value").get.asInstanceOf[String]
           distinctColumnIndex: Int = Integer.parseInt(kpiContentMap.asInstanceOf[Map[String, Any]].get("distinct_column").get.asInstanceOf[String])
      } yield {
        ((countConditionColumn, countConditionColumnValue, distinctColumnIndex), List[String](""))
      }

    val rdd2 = lines.map[(String, List[String])](r => (r(groupColumnIndex), r))

    rdd2.combineByKey[List[((Int, String, Int), List[String])]](
      (v) => {
        val kpiContentArray_ = kpiContentArray
        for {
          kpi <- kpiContentArray_
          if kpi._1._2.contains(v(kpi._1._1))
        } yield ((kpi._1._1, kpi._1._2, kpi._1._3), List(v(kpi._1._3)))
      },
      (c: List[((Int, String, Int), List[String])], v) => {
        for {
          column <- c
        } yield {
          if (column._1._2.contains(v(column._1._1))) {
            ((column._1._1, column._1._2, column._1._3), v(column._1._3) :: column._2)
          } else {
            column
          }
        }
      },
      (c1: List[((Int, String, Int), List[String])], c2: List[((Int, String, Int), List[String])]) => {
        for {
          column1 <- c1
          column2 <- c2
          if column1._1.equals(column2._1)
        } yield
        (column1._1, column1._2 ::: column2._2)
      }
    ).map(kv => {
      val kpis =
        for {
          kpi <- kv._2
        } yield kpi._2.distinct.length
      (kv._1, kpis.toString())
    })
  }

  private def filterDatas(lines: String, rule: DBObject): Boolean = {
    val filterColumnIndex = Integer.parseInt(rule.get("filter_column").toString)
    val filterColumnValues = rule.get("filter_column_value").toString.split(",")
    val filterColumn = lines.split("\t")(filterColumnIndex)
    filterColumnValues.contains(filterColumn)
  }
}
