package com.dsp.statistics

import java.text.SimpleDateFormat
import java.lang.Long
import java.util.{Date, Calendar}
import com.mongodb.DBObject
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import scala.util.parsing.json.JSON


/**
 * caculate kpi
 */
object StatisticsDspByRules {

  val hbaseConf = HBaseConfiguration.create()
  hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
  hbaseConf.set("hbase.zookeeper.quorum", "mastersnn.hadoop,masterjt.hadoop,masternn.hadoop")


  def main(args: Array[String]) {
    val sparkConf = new SparkConf()

    val sc = new SparkContext(sparkConf)

    val lines = sc.textFile(args(0))

    val rules = MongoDBTest.getMongoDBRules

    val jobConf = new JobConf(hbaseConf,this.getClass)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,"HB_dsp_kpi")
    jobConf.set(TableInputFormat.INPUT_TABLE, "HB_dsp_kpi")
    rules.foreach(computeDatas(lines,_).saveAsHadoopDataset(jobConf))
  }


  private def computeDatas(lines: RDD[String], rule: DBObject) = {
    val filteredRdd = lines.filter(filterDatas(_, rule))
    val rdd1 = filteredRdd.map(line => {
      line.split("\t").toList
    })
    combineRddByRule(rdd1, resolveRule(rule))
  }

  private def filterDatas(line: String, rule: DBObject): Boolean = {
    val filterColumnIndex = Integer.parseInt(rule.get(RulesConstants.FILTER_COLUMNS_INDEXES).toString)
    val filterColumnValues = rule.get(RulesConstants.FILTER_COLUMNS_VALUES).toString.split(",")
    val filterColumn = line.split("\t")(filterColumnIndex)
    filterColumnValues.contains(filterColumn)
  }


  private def resolveRule(rule: DBObject):(String,Array[Int],List[((String,Int, String, Int), List[String])],String) = {
    val stringToInt = (string: String) =>{if (string.isEmpty) -1 else Integer.parseInt(string)}
    val bizCode = rule.get(RulesConstants.BIZ_CODE).toString
    val summaryPeriod = rule.get(RulesConstants.SUMMARY_PERIOD).toString
    val groupColumnIndexes = for{groupColumnIndex<-rule.get(RulesConstants.GROUP_COLUMN_INDEXES).toString.split(",")}yield Integer.parseInt(groupColumnIndex)
    val json: Option[Any] = JSON.parseFull(rule.toString)
    val map: Map[String, Any] = json.get.asInstanceOf[Map[String, Any]]
    val statisticsColumns: List[Any] = map.get(RulesConstants.STATISTICS_COLUMNS).get.asInstanceOf[List[Any]]
    val statisticsColumnsRule: List[((String,Int, String, Int), List[String])] =
      for {statisticsColumn <- statisticsColumns
           countType: String = statisticsColumn.asInstanceOf[Map[String, Any]].get(RulesConstants.STATISTICS_COLUMN_TYPE).get.asInstanceOf[String]
           countConditionColumnValues: String = statisticsColumn.asInstanceOf[Map[String, Any]].get(RulesConstants.COUNT_CONDITION_COLUMN_VALUES).get.asInstanceOf[String]
           countConditionColumnIndex: Int = stringToInt(statisticsColumn.asInstanceOf[Map[String, Any]].get(RulesConstants.COUNT_CONDITION_COLUMN_INDEX).get.asInstanceOf[String])
           distinctColumnIndex: Int = stringToInt(statisticsColumn.asInstanceOf[Map[String, Any]].get(RulesConstants.DISTINCT_COLUMN_INDEX).get.asInstanceOf[String])
      } yield {
        ((countType,countConditionColumnIndex, countConditionColumnValues, distinctColumnIndex), List[String]())
      }
    (bizCode,groupColumnIndexes,statisticsColumnsRule,summaryPeriod)
  }


  private def combineRddByRule(lines: RDD[List[String]], resolvedRule: (String,Array[Int],List[((String,Int, String, Int), List[String])],String)) = {

    val rdd2 = lines.map[(String, List[String])](r => {
      val key=
      for{
        groupColumnIndex<-resolvedRule._2
      }yield{
        if(groupColumnIndex == 0)  r(groupColumnIndex).substring(0,2)
        else r(groupColumnIndex)
      }
      (key.toList.addString(new StringBuilder,",").toString(), r)
    })

    val createCombiner = (v :List[String]) => {
      for {
        kpi <- resolvedRule._3
      } yield {
        if(kpi._1._3.isEmpty) (kpi._1, List(v(kpi._1._4))) else{
          if (kpi._1._3.contains(v(kpi._1._2))){
            if(kpi._1._4 == -1) (kpi._1, List("")) else (kpi._1, List(v(kpi._1._4)))
          }else kpi
        }
      }
    }


    val mergeValue = (c: List[((String,Int, String, Int), List[String])], v: List[String]) => {
      for {
        column <- c
      } yield {
        if(column._1._3.isEmpty) (column._1, v(column._1._4) :: column._2) else{
          if (column._1._3.contains(v(column._1._2))){
            if(column._1._4 == -1) (column._1, "" :: column._2) else (column._1, v(column._1._4) :: column._2)
          }else column
        }
      }
    }

    val mergeCombiners = (c1: List[((String,Int, String, Int), List[String])], c2: List[((String,Int, String, Int), List[String])]) => {
      for {
        column1 <- c1
        column2 <- c2
        if column1._1.equals(column2._1)
      } yield (column1._1, column1._2 ::: column2._2)
    }


    rdd2.combineByKey[List[((String,Int, String, Int), List[String])]]( createCombiner,mergeValue,mergeCombiners).map(kv => {
      val kpis =
        for {
          kpi <- kv._2
        } yield kpi._1._1 match{
          case RulesConstants.COUNT_DISTINCT => kpi._2.distinct.length
          case RulesConstants.COUNT => kpi._2.length
        }
      (resolvedRule._1+","+kv._1, kpis.addString(new StringBuilder,",").toString())
    }).map(convert(_,resolvedRule._4))
  }

  private def convert(kpi: (String, String),summaryPeriod: String) = {
    val date = Calendar.getInstance()
    date.setTime(new Date)
    date.add(Calendar.HOUR_OF_DAY,-1)
    if(summaryPeriod.equals(RulesConstants.SUMMARY_PERIOD_DAY)){
      val rowKey = Bytes.toBytes(kpi._1+","+new SimpleDateFormat("yyyyMMdd").format(date.getTime))
      val hbaseConn = ConnectionFactory.createConnection(hbaseConf)
      val table = hbaseConn.getTable(TableName.valueOf("HB_dsp_kpi"))
      val get= new Get(Bytes.toBytes(kpi._1+","+new SimpleDateFormat("yyyyMMdd").format(date.getTime)))
      val result = table.get(get)
      val p = new Put(rowKey)
      var i = 0
      while(i<kpi._2.split(",").length){
        if(result.isEmpty) p.addColumn(Bytes.toBytes("kpiFamily"),Bytes.toBytes(i),Bytes.toBytes(kpi._2.split(",")(i)))
        else{
          val value = Long.parseLong(Bytes.toString(result.getValue(Bytes.toBytes("kpiFamily"),Bytes.toBytes(i))))
          p.addColumn(Bytes.toBytes("kpiFamily"),Bytes.toBytes(i),Bytes.toBytes(String.valueOf(value+Long.parseLong(kpi._2.split(",")(i)))))
        }
        i+=1
      }
      hbaseConn.close()
      (new ImmutableBytesWritable, p)
    }else{
      val rowKey = Bytes.toBytes(kpi._1+","+new SimpleDateFormat("yyyyMMddHH").format(date.getTime))
      val p = new Put(rowKey)
      var i = 0
      while(i<kpi._2.split(",").length){
        p.addColumn(Bytes.toBytes("kpiFamily"),Bytes.toBytes(i),Bytes.toBytes(kpi._2.split(",")(i)))
        i+=1
      }
      (new ImmutableBytesWritable, p)
    }
  }
}
