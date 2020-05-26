package com.alflib.sql.visitor

import com.alflib.sql.utils.CommonUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.expressions.{Expression, ListQuery}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, InsertIntoTable, LogicalPlan, With}
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.execution.command.CreateViewCommand


object LogicalPlanVisitor {
  
  val logger: Logger=Logger.getLogger(getClass)
  
  def visit(node: TreeNode[_], extract: (TreeNode[_]) => Unit, stopList: List[String] = List.empty) : Unit = {
    logger.debug(s"Visiting logical node in ${if (stopList.isEmpty) "Global\t" else "Local\t"}: ${node.nodeName}")
    extract(node)
    if (!stopList.contains(node.nodeName)) {
      node.nodeName match {
        case "CreateViewCommand" => visit(node.asInstanceOf[CreateViewCommand].child, extract)
        case "With" => node.asInstanceOf[With].innerChildren.map(e => visit(e, extract))
        case "Filter" => visit(node.asInstanceOf[Filter].condition, extract, stopList)
        case "ListQuery" => visit(node.asInstanceOf[ListQuery].plan, extract, stopList)
        case _ => null
      }
      node.children.map(e => visit(e.asInstanceOf[TreeNode[_]], extract, stopList))
    }
  }
  
  /*
  def visit(node: Any, extract: (TreeNode[_]) => Unit, stopList: List[String] = List.empty) : Unit = {
    visitNode(node.asInstanceOf[TreeNode[_]], extract, stopList)
  }
  
  def visit(exp: Expression, extract: (LogicalPlan) => Unit, stopList: List[String]) : Unit = {
    logger.debug(s"Visiting expression: ${exp.nodeName}")
    extract(exp)
    if (!stopList.contains(exp.nodeName)) {
      exp.nodeName match {
        case _ => null
      }
      exp.children.map(e => visit(e, extract, stopList))
    }
  }
  */
  
}
