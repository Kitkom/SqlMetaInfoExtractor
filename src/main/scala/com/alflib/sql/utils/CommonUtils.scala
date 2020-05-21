package com.alflib.sql.utils

import com.alflib.sql.exception.ExtractorErrorException
import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.{AliasIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.internal.SQLConf

object CommonUtils {
  
  val logger: Logger=Logger.getLogger(getClass)
  
  def visitMultipleSqls(sqls: String, visit: (LogicalPlan) => Unit) : Unit = {
    val sparkParser = new SparkSqlParser (new SQLConf)
    sqls.split(";").map(it => {
      if (it.trim!="") {
        val plan = sparkParser.parsePlan(it)
        logger.debug(plan)
        visit(plan)
      }
    })
  }
  
  def getIdentifier(src: TableIdentifier) = {
    s"${src.database}.${src.table}"
  }
  
  def getIdentifier(src: AliasIdentifier) = {
    s"${src.database}.${src.identifier}"
  }
  
  def getIdentifier(src: String) = {
    val list = src.replaceAll("`", "").split("\\.")
    if (list.size == 1)
      s"None.${list(0)}"
    else if (list.size == 2)
      s"${list(0)}.${list(1)}"
    else
      throw ExtractorErrorException(s"${src} is not a valid table/view identifier!")
  }
}
