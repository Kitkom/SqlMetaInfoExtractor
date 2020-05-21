package com.alflib.sql.visitor

import org.apache.spark.sql.catalyst.expressions.{Expression, ListQuery}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, With}
import org.apache.spark.sql.execution.command.CreateViewCommand


object LogicalPlanVisitor {
  def visit(plan: With, extractor: (LogicalPlan) => Seq[Any]) : Seq[Any] = {
    visit(plan.innerChildren, extractor)
  }
  
  def visit(plan: CreateViewCommand, extractor: (LogicalPlan) => Seq[Any]) : Seq[Any] = {
    visit(plan.child, extractor)
  }
  
  def visit(plan: LogicalPlan, extractor: (LogicalPlan) => Seq[Any]) : Seq[Any] = {
    var result = Seq[Any]()
    if (plan.nodeName == "CreateViewCommand") result = result ++ visit(plan.asInstanceOf[CreateViewCommand], extractor)
    if (plan.nodeName == "With") result = result ++ visit(plan.asInstanceOf[With], extractor)
    if (plan.nodeName == "Filter") result = result ++ visit(plan.asInstanceOf[Filter].condition, extractor)
    result ++ visit(plan.children, extractor) ++ extractor(plan)
  }
  
  def visit(plans: Seq[LogicalPlan], extractor: (LogicalPlan) => Seq[Any]) : Seq[Any] = {
    var result = Seq[Any]()
    for (e <- plans)
      result = result ++ visit(e, extractor)
    result
  }
  
  def visit(exp: Expression, extractor: (LogicalPlan) => Seq[Any]) : Seq[Any] = {
    var result = Seq[Any]()
    if (exp.nodeName == "ListQuery") result = result ++ visit(exp.asInstanceOf[ListQuery].plan, extractor)
    for (e <- exp.children)
      result = result ++ visit(e, extractor)
    result
  }
}
