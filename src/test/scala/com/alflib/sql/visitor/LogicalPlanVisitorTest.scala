package com.alflib.sql.visitor

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.internal.SQLConf

class LogicalPlanVisitorTest extends org.scalatest.FunSuite {
  test("Basic function: Get table list: With and subquery") {
    
    def getTableList(node:LogicalPlan) : Seq[Any] = {
      if (node.nodeName == "UnresolvedRelation") {
        return Seq(node.argString.replaceAll("`", ""))
      }
      return Seq()
    }
    
    val sql = """
       with vA as (select cA, cB, cC from dA.tC)
       select cA, cB, cC
         from dA.tA
         join vA
        where cA in (select cA from dB.tB)
    """
    val sparkParser = new SparkSqlParser (new SQLConf)
    val plan: LogicalPlan = sparkParser.parsePlan (sql)
    val result = LogicalPlanVisitor.visit(plan, getTableList(_)).asInstanceOf[Seq[String]]
    assert(result.size == 4)
  }
  
}
