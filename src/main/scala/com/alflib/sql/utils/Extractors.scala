package com.alflib.sql.utils

import com.alflib.sql.visitor.LogicalPlanVisitor
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.ListQuery
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, LogicalPlan, Project, SubqueryAlias}
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.execution.command.CreateViewCommand
import org.apache.spark.sql.execution.datasources.CreateTable

object Extractors {
  def extractSourceList(node:TreeNode[_]) : Unit = {
    if (node.nodeName == "UnresolvedRelation")
      GlobalMetaInfo.sourceTableList += TableID.convertArgString(node.argString)
  }
  
  def extractLocalSourceTable(info: QueryUnitInfo)(node: TreeNode[_]) : Unit = {
    node.nodeName match {
      case "Project" => GlobalMetaInfo.setProjectVisited(node.asInstanceOf[Project])
      case "ListQuery" => info.addSource(GlobalMetaInfo.queryUnitInfo(node, "Project").id)
      case "Union" | "Except" => info.addSource(GlobalMetaInfo.queryUnitInfo(node, "Merge").id)
      case "UnresolvedRelation" => info.addSource(TableID.convertArgString(node.argString))
      case "SubqueryAlias" => info.addSource(new TableID(node.asInstanceOf[SubqueryAlias].name))
      case _ => null
    }
  }
  
  val LocalNodeStopList = List("SubqueryAlias", "ListQuery", "UnresolvedRelation", "Union", "Except")
  
  def extractTableLineage(node: TreeNode[_]) : Unit = {
    node.nodeName match {
      case "CreateTable" => {
        val create = node.asInstanceOf[CreateTable]
        val info = GlobalMetaInfo.queryUnitInfo(new TableID(create.tableDesc.identifier), TableLifeType.Table, create)
        create.children.map(child => LogicalPlanVisitor.visit(child, extractLocalSourceTable(info)(_), LocalNodeStopList))
      }
      case "CreateViewCommand" => {
        val create = node.asInstanceOf[CreateViewCommand]
        val info = GlobalMetaInfo.queryUnitInfo(new TableID(create.name), TableLifeType.TempView, create)
        LogicalPlanVisitor.visit(create.child, extractLocalSourceTable(info)(_), LocalNodeStopList)
      }
      case "InsertIntoTable" => {
        val insert = node.asInstanceOf[InsertIntoTable]
        val info = GlobalMetaInfo.queryUnitInfo(TableID.convertArgString(insert.table.asInstanceOf[UnresolvedRelation].argString), TableLifeType.Table, insert)
        insert.children.map(child => LogicalPlanVisitor.visit(child, extractLocalSourceTable(info)(_), LocalNodeStopList))
      }
      case "SubqueryAlias" => {
        val subquery = node.asInstanceOf[SubqueryAlias]
        val info = GlobalMetaInfo.queryUnitInfo(new TableID(subquery.name), TableLifeType.SubQueryAias, subquery)
        LogicalPlanVisitor.visit(subquery.child, extractLocalSourceTable(info)(_), LocalNodeStopList)
      }
      case "ListQuery" => {
        val listquery = node.asInstanceOf[ListQuery]
        val info = GlobalMetaInfo.queryUnitInfo(node, "Project")
        LogicalPlanVisitor.visit(listquery.plan, extractLocalSourceTable(info)(_), LocalNodeStopList)
      }
      case "Union" | "Except" => {
        val info = GlobalMetaInfo.queryUnitInfo(node, "Merge")
        node.asInstanceOf[LogicalPlan].children.map(child => LogicalPlanVisitor.visit(child, extractLocalSourceTable(info)(_), LocalNodeStopList))
      }
      case "Project" => {
        val project = node.asInstanceOf[Project]
        if (!GlobalMetaInfo.checkProjectVisited(project)) {
          val info = GlobalMetaInfo.queryUnitInfo(project, "Project")
          //project.children.map(x => LogicalPlanVisitor.visit(x, extractLocalSourceTable(info)(_), LocalNodeStopList))
          LogicalPlanVisitor.visit(project, extractLocalSourceTable(info)(_), LocalNodeStopList)
          
        }
      }
      case _ => null
    }
  }
  
  def extractColumnLineage(node: TreeNode[_]) : Unit = {
  }
  
  def extractMetaInfo(node: TreeNode[_]) : Unit = {
    extractSourceList(node)
    extractTableLineage(node)
    extractColumnLineage(node)
  }
}
