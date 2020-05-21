package com.alflib.sql.visitor

import com.alflib.sql.utils.{CommonUtils, GlobalMetaInfo}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.internal.SQLConf

class LogicalPlanVisitorTest extends org.scalatest.FunSuite {
  
  val logger: Logger=CommonUtils.logger
  
  {
    logger.setLevel(Level.DEBUG)
  }
  
  test("Basic function: Get table list: With and subquery") {
    val sql =
    """
       with vA as (select cA, cB, cC from dA.tC)
       select cA, cB, cC
         from dA.tA
         join vA
         join (select cA, cB, cC from dA.tD)
         join (select cA, cB, cC from dA.tE) vB
        where cA in (select cA from dB.tB)
          and cB in (select cB from vB)
    """
    GlobalMetaInfo.clear
    CommonUtils.visitMultipleSqls(sql, x => LogicalPlanVisitor.visit(x, GlobalMetaInfo.extractMetaInfo(_)))
    GlobalMetaInfo.cleanUp
    logger.debug(GlobalMetaInfo.sourceTableList)
    logger.debug(GlobalMetaInfo.tempViewMap)
    assert(GlobalMetaInfo.sourceTableList.size == 5)
    assert(GlobalMetaInfo.tempViewMap.size == 3)
  }
  
  test("Basic function: Get table list: create temporary views and tables") {
    val sql =
    """
       create temporary view tvA as
         select cA, cB, cC
           from dA.tA
           join dA.tB;
           
       select cA, cB, cC
         from dB.tC
         join tvA;
    """
    
    GlobalMetaInfo.clear
    CommonUtils.visitMultipleSqls(sql, x => LogicalPlanVisitor.visit(x, GlobalMetaInfo.extractMetaInfo(_)))
    GlobalMetaInfo.cleanUp
    logger.debug(GlobalMetaInfo.sourceTableList)
    logger.debug(GlobalMetaInfo.tempViewMap)
    assert(GlobalMetaInfo.sourceTableList.size == 3)
    assert(GlobalMetaInfo.tempViewMap.size == 1)
  }
  
}
