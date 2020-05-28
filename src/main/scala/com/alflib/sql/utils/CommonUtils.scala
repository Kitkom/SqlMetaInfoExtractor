package com.alflib.sql.utils

import com.alflib.sql.exception.ExtractorErrorException
import com.alflib.sql.visitor.LogicalPlanVisitor
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
  val replaceListSeparator = "\t"
  
  {
    for (line <- Source.fromResource("replace.list").getLines.filterNot(_.trim.startsWith("#"))) {
      val d = line.split(replaceListSeparator)
      replaceList(d(0))=d(1)
    }
  }
  
  def visitFile(path: String, visitor: (LogicalPlan) => Unit) : Unit = {
    logger.info(s"Visiting file ${path}")
    val sql = new StringBuilder
    for (line<-Source.fromFile(path, "utf-8").getLines)
      if (!line.trim.startsWith("--"))
        sql.append(line+"\n")
    CommonUtils.visitMultipleSqls(sql.toString, visitor)
  }
  
  def visitMultipleSqls(sqls: String, visitor: (LogicalPlan) => Unit) : Unit = {
    val sparkParser = new SparkSqlParser (new SQLConf)
    var formatedSql = sqls
    replaceList.map{case(key, value)=>{formatedSql = formatedSql.replace(key, value)}}
    formatedSql.split(";").map(it => {
      if (it.trim!="") {
    //    try {
          val plan = sparkParser.parsePlan(it)
          logger.debug(plan)
          visitor(plan)
        /*
        }
        catch {
          case e : Exception => GlobalMetaInfo.errors(it.trim) = e
          case _ : Throwable => GlobalMetaInfo.errors(it.trim) = ExtractorErrorException("Unsupported query")
        }
        */
      }
    })
  }
  
}
