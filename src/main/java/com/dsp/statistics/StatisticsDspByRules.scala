package com.dsp.statistics

import com.mongodb.DBObject
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import scala.util.parsing.json.JSON

/**
 * Created by lixiang on 2015/8/19.
 */
object StatisticsDspByRules {

  def main(args: Array[String]) {
    val conf = new SparkConf()

    val sc = new SparkContext(conf)

    val lines = sc.textFile(args(0))

    val rules = MongoDBTest.getMongoDBRules()

    rules.foreach(rule => {
      computeDatas(lines, rule).saveAsTextFile(args(1)+rule.get("biz_code").toString)
    })


  }

  private def computeDatas(lines: RDD[String], rule: DBObject): RDD[(String, String)] = {
    val filtedRdd = lines.filter(filterDatas(_, rule))
    val rdd1 = filtedRdd.map(line => {
      line.split("\t").toList
    })
    resolveRule(rdd1, rule)
  }



  private def resolveRule(lines: RDD[List[String]], rule: DBObject): RDD[(String, String)] = {

    val stringToInt = (string: String) =>{if (string.isEmpty) 0 else Integer.parseInt(string)}

    val groupColumnIndexs = rule.get("group_columns").toString.split(",").foreach(Integer.parseInt)
    val json: Option[Any] = JSON.parseFull(rule.toString)
    val map: Map[String, Any] = json.get.asInstanceOf[Map[String, Any]]
    val kpiContent: List[Any] = map.get("kpi_content").get.asInstanceOf[List[Any]]
    val kpiContentArray: List[((String,Int, String, Int), List[String])] =
      for {kpiContentMap <- kpiContent
           countType: String = kpiContentMap.asInstanceOf[Map[String, Any]].get("type").get.asInstanceOf[String]
           countConditionColumnValue: String = kpiContentMap.asInstanceOf[Map[String, Any]].get("count_condition_column_value").get.asInstanceOf[String]
           countConditionColumn: Int = stringToInt(kpiContentMap.asInstanceOf[Map[String, Any]].get("count_condition_column").get.asInstanceOf[String])
           distinctColumnIndex: Int = stringToInt(kpiContentMap.asInstanceOf[Map[String, Any]].get("distinct_column").get.asInstanceOf[String])
      } yield {
        ((countType,countConditionColumn, countConditionColumnValue, distinctColumnIndex), List[String]())
      }

    val rdd2 = lines.map[(String, List[String])](r => {
      val key: List[String]=
      for{
        groupColumnIndex<-groupColumnIndexs
      }yield r(groupColumnIndex)

      (key.toString, r)
    })

    rdd2.combineByKey[List[((String,Int, String, Int), List[String])]](
      (v :List[String]) => {
        val kpiContentArray_ = kpiContentArray
        for {
          kpi <- kpiContentArray_
        } yield {
          if(kpi._1._3.isEmpty){
            (kpi._1, List(v(kpi._1._4)))
          }else{
            if (kpi._1._3.contains(v(kpi._1._2))){
              (kpi._1, List(v(kpi._1._4)))
            }else{
              kpi
            }
          }
        }
      },
      (c: List[((String,Int, String, Int), List[String])], v: List[String]) => {
        for {
          column <- c
        } yield {
          if(column._1._3.isEmpty){
            (column._1, v(column._1._4) :: column._2)
          }else{
            if (column._1._3.contains(v(column._1._2))){
              (column._1, v(column._1._4) :: column._2)
            }else {
              column
            }
          }
        }
      },
      (c1: List[((String,Int, String, Int), List[String])], c2: List[((String,Int, String, Int), List[String])]) => {
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
        } yield kpi._1._1 match{
          case "count_distinct" =>{kpi._2.distinct.length}
          case "count" =>{kpi._2.length}
        }
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
