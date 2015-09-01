package com.dsp.statistics

import java.text.SimpleDateFormat
import java.util.Date

import com.mongodb.DBObject
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, ConnectionFactory}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import scala.util.parsing.json.JSON

/**
 * Created by lixiang on 2015/8/19.
 */

/**
 * combineBykey
 */
object StatisticsDspByRules {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf()

    val sc = new SparkContext(sparkConf)

    val lines = sc.textFile(args(0))

    val rules = MongoDBTest.getMongoDBRules()

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "mastersnn.hadoop,masterjt.hadoop,masternn.hadoop")
    val jobConf = new JobConf(conf,this.getClass)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,"HB_dsp_kpi")
    jobConf.set(TableInputFormat.INPUT_TABLE, "HB_dsp_kpi")
    rules.foreach(rule => {
      computeDatas(lines, rule).saveAsHadoopDataset(jobConf)
    })

  }


  private def computeDatas(lines: RDD[String], rule: DBObject) = {
    val filtedRdd = lines.filter(filterDatas(_, rule))
    val rdd1 = filtedRdd.map(line => {
      line.split("\t").toList
    })
    resolveRule(rdd1, rule)
  }



  private def resolveRule(lines: RDD[List[String]], rule: DBObject) = {

    val stringToInt = (string: String) =>{if (string.isEmpty) -1 else Integer.parseInt(string)}
    val bizCode = rule.get("bizCode").toString
    val groupColumnIndexs = for{groupColumnIndex<-rule.get("groupColumns").toString.split(",")}yield Integer.parseInt(groupColumnIndex)
    val json: Option[Any] = JSON.parseFull(rule.toString)
    val map: Map[String, Any] = json.get.asInstanceOf[Map[String, Any]]
    val kpiContent: List[Any] = map.get("kpiContent").get.asInstanceOf[List[Any]]
    val kpiContentArray: List[((String,Int, String, Int), List[String])] =
      for {kpiContentMap <- kpiContent
           countType: String = kpiContentMap.asInstanceOf[Map[String, Any]].get("type").get.asInstanceOf[String]
           countConditionColumnValue: String = kpiContentMap.asInstanceOf[Map[String, Any]].get("countConditionColumnValue").get.asInstanceOf[String]
           countConditionColumn: Int = stringToInt(kpiContentMap.asInstanceOf[Map[String, Any]].get("countConditionColumn").get.asInstanceOf[String])
           distinctColumnIndex: Int = stringToInt(kpiContentMap.asInstanceOf[Map[String, Any]].get("distinctColumn").get.asInstanceOf[String])
      } yield {
        ((countType,countConditionColumn, countConditionColumnValue, distinctColumnIndex), List[String]())
      }

    val rdd2 = lines.map[(String, List[String])](r => {
      val key=
      for{
        groupColumnIndex<-groupColumnIndexs
      }yield{
        if(groupColumnIndex == 0)  r(groupColumnIndex).substring(0,2)
        else r(groupColumnIndex)
      }
      (key.toList.addString(new StringBuilder,",").toString, r)
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
              if(kpi._1._4 == -1) (kpi._1, List("")) else (kpi._1, List(v(kpi._1._4)))
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
              if(column._1._4 == -1) (column._1, "" :: column._2) else (column._1, v(column._1._4) :: column._2)
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
      (bizCode+","+kv._1, kpis.addString(new StringBuilder,",").toString)
    }).map(convert)
  }

  def convert(kpi: (String, String)) = {
    val p = new Put(Bytes.toBytes(kpi._1+","+new SimpleDateFormat("yyyyMMddHH").format(new Date())))
    var i = 0
    while(i<kpi._2.split(",").length){
      p.addColumn(Bytes.toBytes("kpiFamily"),Bytes.toBytes(String.valueOf(i)),Bytes.toBytes(kpi._2.split(",")(i)))
      i+=1
    }
    (new ImmutableBytesWritable, p)
  }

  private def filterDatas(lines: String, rule: DBObject): Boolean = {
    val filterColumnIndex = Integer.parseInt(rule.get("filterColumn").toString)
    val filterColumnValues = rule.get("filterColumnValue").toString.split(",")
    val filterColumn = lines.split("\t")(filterColumnIndex)
    filterColumnValues.contains(filterColumn)
  }
}
