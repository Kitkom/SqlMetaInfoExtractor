package com.alflib.sql.visitor

import com.alflib.sql.utils.CommonUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.expressions.{Expression, ListQuery}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, InsertIntoTable, LogicalPlan, With}
import org.apache.spark.sql.execution.command.CreateViewCommand


object LogicalPlanVisitor {
  
  val logger: Logger=Logger.getLogger(getClass)
  
  def visit(plan: LogicalPlan, extract: (LogicalPlan) => Unit, list: List[String] = List.empty) : Unit = {
    logger.debug(s"Visiting logical plan: ${plan.nodeName}")
    extract(plan)
    if (list.isEmpty || list.contains(plan.nodeName)) {
    plan.nodeName match {
      case "CreateViewCommand" => visit(plan.asInstanceOf[CreateViewCommand].child, extract)
      case "With" => plan.asInstanceOf[With].innerChildren.map(e => visit(e, extract))
      case "Filter" => visit(plan.asInstanceOf[Filter].condition, extract, list)
      case _ => null
    }
      plan.children.map(e => visit(e, extract, list))
    }
  }
  
  def visit(exp: Expression, extract: (LogicalPlan) => Unit, list: List[String]) : Unit = {
    logger.debug(s"Visiting expression: ${exp.nodeName}")
    exp.nodeName match {
      case "ListQuery" => visit(exp.asInstanceOf[ListQuery].plan, extract, list)
      case _ => null
    }
    exp.children.map(e => visit(e, extract, list))
  }
  
}
