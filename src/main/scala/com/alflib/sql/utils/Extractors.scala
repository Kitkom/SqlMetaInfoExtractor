package com.alflib.sql.utils

import com.alflib.sql.visitor.LogicalPlanVisitor
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, LogicalPlan, Project, SubqueryAlias}
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.execution.command.CreateViewCommand
import org.apache.spark.sql.execution.datasources.CreateTable

object Extractors {
  def extractSourceList(node:TreeNode[_]) : Unit = {
    if (node.nodeName == "UnresolvedRelation")
      GlobalMetaInfo.sourceTableList += TableID.convertArgString(node.argString)
  }
  
  def extractLocalSourceTable(tableInfo: QueryUnitInfo)(node: TreeNode[_]) : Unit = {
    node.nodeName match {
      case "Project" => {
        val project = node.asInstanceOf[Project]
        if (tableInfo.logicalPlan == null)
          tableInfo.logicalPlan = project
        else {
          val info = GlobalMetaInfo.getQueryUnitInfo(project)
          tableInfo.sources(info.id) = info
        }
      }
      case "UnresolvedRelation" => tableInfo.addSource(GlobalMetaInfo.getQueryUnitInfo(TableID.convertArgString(node.argString)))
      case "SubqueryAlias" => tableInfo.addSource(GlobalMetaInfo.getQueryUnitInfo(new TableID(node.asInstanceOf[SubqueryAlias].name)))
      case _ => null
    }
  }
  
  val LocalNodeStopList = List("SubqueryAlias", "UnresolvedRelation", "ListQuery")
  
  def extractTableLineage(node: TreeNode[_]) : Unit = {
    node.nodeName match {
      case "CreateTable" => {
        val create = node.asInstanceOf[CreateTable]
        val info = GlobalMetaInfo.getQueryUnitInfo(new TableID(create.tableDesc.identifier), TableLifeType.Table)
        create.children.map(x => LogicalPlanVisitor.visit(x, extractLocalSourceTable(info)(_), LocalNodeStopList))
      }
      case "CreateViewCommand" => {
        val create = node.asInstanceOf[CreateViewCommand]
        val info = GlobalMetaInfo.getQueryUnitInfo(new TableID(create.name), TableLifeType.TempView)
        LogicalPlanVisitor.visit(create.child, extractLocalSourceTable(info)(_), LocalNodeStopList)
      }
      case "InsertIntoTable" => {
        val insert = node.asInstanceOf[InsertIntoTable]
        val info = GlobalMetaInfo.getQueryUnitInfo(TableID.convertArgString(insert.table.asInstanceOf[UnresolvedRelation].argString), TableLifeType.Table)
        insert.children.map(child => LogicalPlanVisitor.visit(child, extractLocalSourceTable(info)(_), LocalNodeStopList))
      }
      case "SubqueryAlias" => {
        val subquery = node.asInstanceOf[SubqueryAlias]
        val info = GlobalMetaInfo.getQueryUnitInfo(new TableID(subquery.name), TableLifeType.SubQueryAias)
        LogicalPlanVisitor.visit(subquery.child, extractLocalSourceTable(info)(_), LocalNodeStopList)
      }
      case "Project" => {
        val project = node.asInstanceOf[Project]
        if (!GlobalMetaInfo.checkProjectRegistered(project)) {
          val info = GlobalMetaInfo.getQueryUnitInfo(project)
          project.children.map(x => LogicalPlanVisitor.visit(x, extractLocalSourceTable(info)(_), LocalNodeStopList))
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
