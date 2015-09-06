package com.dsp.statistics

import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoCursor
import scala.util.parsing.json._
object MongoDBTest {


  def getMongoDBRules: MongoCursor ={
    val mongoClient = MongoClient("123.57.255.204", 27017)
    val db = mongoClient("dsp_info")
    val rulesCollection = db("JY_statistics_rules")
    rulesCollection.find()
  }

  def main (args: Array[String]) {
    val mongoClient = MongoClient("123.57.255.204", 27017)
    val db = mongoClient("dsp_info")
    val rulesCollection: MongoCollection = db("JY_statistics_rules")
    val json:Option[Any] = JSON.parseFull(rulesCollection.find().next().toString)
    val map:Map[String,Any] = json.get.asInstanceOf[Map[String, Any]]
    val languages:List[Any] = map.get("kpiContent").get.asInstanceOf[List[Any]]
    val distinctColumn =
    for{
      langMap<-languages
       distinctColumns:String = langMap.asInstanceOf[Map[String,Any]].get("distinctColumn").get.asInstanceOf[String]
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
