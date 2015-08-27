package com.dsp.statistics

import com.mongodb.DBObject
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import scala.util.parsing.json.JSON

/**
 * Created by tianpo on 2015/8/27.
 */
object ReduceBykeyTest {
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

    val stringToInt = (string: String) => {
      if (string.isEmpty) -1 else Integer.parseInt(string)
    }

    val groupColumnIndexs = rule.get("group_columns").toString.split(",").foreach(Integer.parseInt)
    val json: Option[Any] = JSON.parseFull(rule.toString)
    val map: Map[String, Any] = json.get.asInstanceOf[Map[String, Any]]
    val kpiContent: List[Any] = map.get("kpi_content").get.asInstanceOf[List[Any]]
    val kpiContentArray: List[(String, Int, String, Int)] =
      for {kpiContentMap <- kpiContent
           countType: String = kpiContentMap.asInstanceOf[Map[String, Any]].get("type").get.asInstanceOf[String]
           countConditionColumnValue: String = kpiContentMap.asInstanceOf[Map[String, Any]].get("count_condition_column_value").get.asInstanceOf[String]
           countConditionColumn: Int = stringToInt(kpiContentMap.asInstanceOf[Map[String, Any]].get("count_condition_column").get.asInstanceOf[String])
           distinctColumnIndex: Int = stringToInt(kpiContentMap.asInstanceOf[Map[String, Any]].get("distinct_column").get.asInstanceOf[String])
      } yield {
        (countType, countConditionColumn, countConditionColumnValue, distinctColumnIndex)
      }

    val rdd2 = lines.map[(String, String)](r => {
      val key: List[String]=
        for{
          groupColumnIndex<-groupColumnIndexs
        }yield r(groupColumnIndex)

      (key.toString, r.toString())
    })

    rdd2.reduceByKey((log1,log2)=>{
      val a =
      for{
        kpi <- kpiContentArray
      }yield{
        if(kpi._3.isEmpty){
          log1(kpi._4)+","+log2(kpi._4)
        }else if(kpi._3.contains(log1(kpi._2))&&kpi._3.contains(log2(kpi._2))&&log1(kpi._2).equals(log2(kpi._2))){
          if(kpi._4 == -1){
            "-,-"
          }else{
            log1(kpi._4)+","+log2(kpi._4)
          }
        }
      }
      a.toString()
    }).map(kv => {
      (kv._1, kv.toString())
    })
  }
  private def filterDatas(lines: String, rule: DBObject): Boolean = {
    val filterColumnIndex = Integer.parseInt(rule.get("filter_column").toString)
    val filterColumnValues = rule.get("filter_column_value").toString.split(",")
    val filterColumn = lines.split("\t")(filterColumnIndex)
    filterColumnValues.contains(filterColumn)
  }

}
