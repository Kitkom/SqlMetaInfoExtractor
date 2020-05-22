package com.alflib.sql.utils

import com.alflib.sql.visitor.LogicalPlanVisitor
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, LogicalPlan, Project, SubqueryAlias}
import org.apache.spark.sql.execution.command.CreateViewCommand

object Extractors {
  def extractSourceList(node:LogicalPlan) : Unit = {
    if (node.nodeName == "UnresolvedRelation")
      GlobalMetaInfo.sourceTableList += TableID.convertArgString(node.argString)
  }
  
  def extractLocalSourceTable(tableInfo: QueryUnitInfo)(node: LogicalPlan) : Unit = {
    node.nodeName match {
      case "Project" => tableInfo.logicalPlan = node.asInstanceOf[Project]
      case "UnresolvedRelation" => tableInfo.addSource(GlobalMetaInfo.getQueryUnitInfo(TableID.convertArgString(node.argString)))
      case "SubqueryAlias" => tableInfo.addSource(GlobalMetaInfo.getQueryUnitInfo(new TableID(node.asInstanceOf[SubqueryAlias].name)))
      case _ => null
    }
  }
  
  val LocalNodeList = List("Join", "Project", "With", "Filter", "Union")
  
  def extractTableLineage(node: LogicalPlan) : Unit = {
    node.nodeName match {
      case "CreateViewCommand" => {
        val create = node.asInstanceOf[CreateViewCommand]
        val info = GlobalMetaInfo.getQueryUnitInfo(new TableID(create.name), TableLifeType.TempView)
        LogicalPlanVisitor.visit(create.child, extractLocalSourceTable(info)(_), LocalNodeList)
      }
      case "InsertIntoTable" => {
        val insert = node.asInstanceOf[InsertIntoTable]
        val info = GlobalMetaInfo.getQueryUnitInfo(TableID.convertArgString(insert.table.asInstanceOf[UnresolvedRelation].argString), TableLifeType.Table)
        insert.children.map(child => LogicalPlanVisitor.visit(child, extractLocalSourceTable(info)(_), LocalNodeList))
      }
      case "SubqueryAlias" => {
        val subquery = node.asInstanceOf[SubqueryAlias]
        val info = GlobalMetaInfo.getQueryUnitInfo(new TableID(subquery.name), TableLifeType.SubQueryAias)
        LogicalPlanVisitor.visit(subquery.child, extractLocalSourceTable(info)(_), LocalNodeList)
      }
      case "Project" => {
        val project = node.asInstanceOf[Project]
        if (!GlobalMetaInfo.checkProjectRegistered(project)) {
          val info = GlobalMetaInfo.getQueryUnitInfo(project)
          LogicalPlanVisitor.visit(project, extractLocalSourceTable(info)(_), LocalNodeList)
        }
      }
      case _ => null
    }
  }
  
  def extractColumnLineage(node: LogicalPlan) : Unit = {
  }
  
  def extractMetaInfo(node: LogicalPlan) : Unit = {
    extractSourceList(node)
    extractTableLineage(node)
    extractColumnLineage(node)
  }
}
