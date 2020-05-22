package com.alflib.sql.visitor

import com.alflib.sql.utils.{CommonUtils, Extractors, GlobalMetaInfo, TableID}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.internal.SQLConf

class LogicalPlanVisitorTest extends org.scalatest.FunSuite {
  
  
  val logger: Logger=Logger.getLogger(getClass)
  
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
    CommonUtils.visitMultipleSqls(sql, x => LogicalPlanVisitor.visit(x, Extractors.extractMetaInfo(_)))
    GlobalMetaInfo.cleanUp
    assert(GlobalMetaInfo.getSources.size == 5)
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
    CommonUtils.visitMultipleSqls(sql, x => LogicalPlanVisitor.visit(x, Extractors.extractMetaInfo(_)))
    GlobalMetaInfo.cleanUp
    assert(GlobalMetaInfo.getSources.size == 3)
  }
  
  test("Basic function: Get table list: table lineage") {
    val sql =
    """
       create temporary view tvA as
         select cA, cB, cC
           from dA.tA
           join dA.tB;
       
       insert into table tZ
       select cA, cB, cC
         from dB.tC
         join tvA
         join (select cA, cB, cC from dC.tD)
         ;
    """
    
    GlobalMetaInfo.clear
    CommonUtils.visitMultipleSqls(sql, x => LogicalPlanVisitor.visit(x, Extractors.extractMetaInfo(_)))
    GlobalMetaInfo.cleanUp
    assert(GlobalMetaInfo.getQueryUnitInfo(TableID.convertArgString("tZ")).getSourceTables.size == 4)
  }
  
}
