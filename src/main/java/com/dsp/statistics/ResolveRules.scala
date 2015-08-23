package com.dsp.statistics

/**
 * Created by tianpo on 2015/8/17.
 */

import com.mongodb.DBObject
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

object ResolveRules {




  def main(args: Array[String]) {

    val conf = new SparkConf()

    val sc = new SparkContext(conf)

    val lines = sc.textFile(args(0))

    val rules = MongoDBTest.getMongoDBRules()

    rules.foreach({
      computeDatas(lines, _).saveAsTextFile(args(1))
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
        ((countConditionColumn, countConditionColumnValue, distinctColumnIndex), List[String]())
      }

    val rdd2 = lines.map[(String,List[String])](r => ((r(groupColumnIndex)), r))




    val combOp = (aKpis: List[((Int, String, Int), List[String])], bKpis: List[((Int, String, Int), List[String])]) => {
      val totalKpi: List[((Int, String, Int), List[String])] =
        for {
          a <- aKpis
          b <- bKpis
        } yield {
            ((a._1), a._2 ::: b._2)
        }
      totalKpi
    }

    rdd2.aggregateByKey(kpiContentArray)(
      (kpis: List[((Int, String, Int), List[String])], kpi: List[String]) => {
        for{
          kpiContent<-kpis
        }yield {
          if(kpiContent._1._2.contains(kpi(kpiContent._1._1))){
            if(kpiContent._2.length<1){
              ((kpiContent._1),List(kpi(kpiContent._1._3)))
            }else{
              ((kpiContent._1),kpi(kpiContent._1._3)::kpiContent._2)
            }
          }else{
            kpiContent
          }
        }
      },
      (kpis1: List[((Int, String, Int), List[String])],kpis2: List[((Int, String, Int), List[String])]) =>{
        /*val conf = new SparkConf()
        conf.setMaster("yarn-cluster")
        conf.setAppName("combineTest")
        val sc = new SparkContext(conf)
         sc.parallelize(kpis1:::kpis2).map(r=>(r._1,r._2)).reduceByKey(_:::_).collect.toList
        */
        for{
          kpi1<-kpis1
        }yield {
          val list =
          for{
            kpi2<-kpis2
            if(kpi1._1.equals(kpi2._1))
          }yield (kpi1._1,kpi1._2:::kpi2._2)
          list(0)
        }
      }
    ).map[(String, String)](kpis => {
      val totalKpi =
        for {
          kpi <- kpis._2
        } yield kpi._2.distinct.length
      (kpis._1, totalKpi.toString)
    })
  }

  private def filterDatas(lines: String, rule: DBObject): Boolean = {
    val filterColumnIndex = Integer.parseInt(rule.get("filter_column").toString)
    val filterColumnValues = rule.get("filter_column_value").toString.split(",")
    val filterColumn = lines.split("\t")(filterColumnIndex)
    filterColumnValues.contains(filterColumn)
  }
}
