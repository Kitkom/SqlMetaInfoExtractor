package com.alflib.sql.utils

import com.alflib.sql.exception.ExtractorErrorException
import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.IdentifierWithDatabase
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.internal.SQLConf

object CommonUtils {
  
  val logger: Logger=Logger.getLogger(getClass)
  
  def visitMultipleSqls(sqls: String, visit: (LogicalPlan) => Unit) : Unit = {
    val sparkParser = new SparkSqlParser (new SQLConf)
    sqls.split(";").map(it => {
      if (it.trim!="") {
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
