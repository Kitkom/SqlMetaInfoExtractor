package com.alflib.sql.utils

import com.alflib.sql.exception.ExtractorErrorException
import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.IdentifierWithDatabase
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.internal.SQLConf
import scala.collection.mutable.{ListBuffer, Map}

import scala.io.Source

object CommonUtils {
  
  val logger: Logger=Logger.getLogger(getClass)
  
  val replaceList = Map[String, String]()
  
  {
    for (line <- Source.fromResource("replace.list").getLines) {
      val d = line.split(' ')
      replaceList(d(0))=d(1)
    }
  }
  
  def visitMultipleSqls(sqls: String, visit: (LogicalPlan) => Unit) : Unit = {
    val sparkParser = new SparkSqlParser (new SQLConf)
    var formatedSql = sqls
    replaceList.map{case(key, value)=>{formatedSql = formatedSql.replace(key, value)}}
    formatedSql.split(";").map(it => {
      if (it.trim!="" && (!it.trim.startsWith("--"))) {
        try {
          val plan = sparkParser.parsePlan(it)
          logger.debug(plan)
          visit(plan)
        }
        catch {
          case e : Exception => GlobalMetaInfo.errors(it.trim) = e
          case _ : Throwable => GlobalMetaInfo.errors(it.trim) = ExtractorErrorException("Unsupported query")
        }
      }
    })
  }
  
}
