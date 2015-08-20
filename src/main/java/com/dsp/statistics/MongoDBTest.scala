package com.dsp.statistics

/**
 * Created by tianpo on 2015/8/17.
 */

import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoCursor
import scala.util.parsing.json._
object MongoDBTest {


  def getMongoDBRules () : MongoCursor ={
    val mongoClient = MongoClient("192.168.100.28", 27017)
    val db = mongoClient("dsp_info")
    val rulesCollection = db("JY_statistics_rules")
    rulesCollection.find()
  }

  def main (args: Array[String]) {
    val mongoClient = MongoClient("192.168.100.28", 27017)
    val db = mongoClient("dsp_info")
    val rulesCollection = db("9yu_statistics_rules")
    val json:Option[Any] = JSON.parseFull(rulesCollection.find().next().toString)
    val map:Map[String,Any] = json.get.asInstanceOf[Map[String, Any]]
    val languages:List[Any] = map.get("kpi_content").get.asInstanceOf[List[Any]]
    val distinctColumn =
    for{
      langMap<-languages
       distinctColumns:String = langMap.asInstanceOf[Map[String,Any]].get("distinct_column").get.asInstanceOf[String]
    }yield {
      distinctColumns
    }
    distinctColumn.toSet[String].foreach(
      indexs=>{
        print(indexs+",")
      }
    )
  }
}
