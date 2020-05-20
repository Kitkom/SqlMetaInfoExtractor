package com.alflib.sql.test

import com.alflib.sql.visitor.LogicalPlanVisitor
import org.apache.spark.sql.catalyst.expressions.{Expression, ListQuery}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, With}
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.execution.command.CreateViewCommand
import org.apache.spark.sql.internal.SQLConf

import scala.collection.mutable.ListBuffer


object SqlMetaInfoExtractorTestDriver {
  
  def getTableList(node:LogicalPlan) : Seq[Any] = {
    if (node.nodeName == "UnresolvedRelation") {
      return Seq(node.argString.replaceAll("`", ""))
    }
    return Seq()
  }
  
  def main(args:Array[String]) : Unit = {
    val sql = """
    with view_a as (
    select col_a, col_b, col_c from tbl_a
    ),
    view_b as (
      select col_b, col_c from tbl_a
    )

    insert into table tbl_z
    select * from view_a
    join tbl_c
    join (select col_d, col_e+col_f from tbl_d join tbl_e join tbl_f on a=b)
    union
    select * from tbl_y
    """
    printPlan(sql)
    val sparkParser = new SparkSqlParser (new SQLConf)
    
    
    val plan: LogicalPlan = sparkParser.parsePlan (sql)
    print(LogicalPlanVisitor.visit(plan, getTableList(_)).asInstanceOf[Seq[String]])
  }
  
  def printPlan(sql:String) : Unit = {
    val sparkParser = new SparkSqlParser (new SQLConf)
    val plan: LogicalPlan = sparkParser.parsePlan (sql)
    println(plan)
  }
  
  def getSrcTblList(sql:String) : List[String] = {
    val sparkParser = new SparkSqlParser (new SQLConf)
    val plan: LogicalPlan = sparkParser.parsePlan (sql)
    visit(plan).toList.distinct
  }
  
  def visit(n:LogicalPlan) : ListBuffer[String] = {
    var r: ListBuffer[String] = ListBuffer()
    if (n.nodeName == "UnresolvedRelation")
      r += n.argString.replaceAll("`", "")
    else if (n.nodeName == "CreateViewCommand")
      for (e <- visit(n.asInstanceOf[CreateViewCommand].child) )
        r += e
    else if (n.nodeName == "Filter")
      for (e <- visitExpression(n.asInstanceOf[Filter].condition) )
        r += e
    else if (n.nodeName == "With")
      for (e <- n.asInstanceOf[With].innerChildren )
        for (s <- visit(e))
          r += s
    for (node <- n.children) for (e <- visit(node)) r += e
    r
  }
  
  def visitExpression(n:Expression) : ListBuffer[String] = {
    var r: ListBuffer[String] = ListBuffer()
    if (n.nodeName == "ListQuery")
      for (e <- visit(n.asInstanceOf[ListQuery].plan))
        r += e
    for (node <- n.children)
      for (e <- visitExpression(node))
        r += e
    r
  }
}
