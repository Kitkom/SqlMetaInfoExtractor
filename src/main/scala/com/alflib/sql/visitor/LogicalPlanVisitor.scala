package com.alflib.sql.visitor

import com.alflib.sql.utils.CommonUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.expressions.{Expression, ListQuery}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, With}
import org.apache.spark.sql.execution.command.CreateViewCommand


object LogicalPlanVisitor {
  
  val logger: Logger=CommonUtils.logger
  
  def visit(plan: With, extract: (LogicalPlan) => Unit) : Unit = {
    for (e <- plan.innerChildren)
      visit(e, extract)
  }
  
  def visit(plan: CreateViewCommand, extract: (LogicalPlan) => Unit) : Unit = {
    visit(plan.child, extract)
  }
  
  def visit(plan: LogicalPlan, extract: (LogicalPlan) => Unit) : Unit = {
    logger.debug(s"Visiting logical plan: ${plan.nodeName}")
    extract(plan)
    if (plan.nodeName == "CreateViewCommand") visit(plan.asInstanceOf[CreateViewCommand], extract)
    if (plan.nodeName == "With") visit(plan.asInstanceOf[With], extract)
    if (plan.nodeName == "Filter") visit(plan.asInstanceOf[Filter].condition, extract)
    for (e <- plan.children)
      visit(e, extract)
  }
  
  def visit(exp: Expression, extract: (LogicalPlan) => Unit) : Unit = {
    logger.debug(s"Visiting expression: ${exp.nodeName}")
    if (exp.nodeName == "ListQuery") visit(exp.asInstanceOf[ListQuery].plan, extract)
    for (e <- exp.children)
      visit(e, extract)
  }
  
}
