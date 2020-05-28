package com.alflib.sql.utils

import com.alflib.sql.visitor.LogicalPlanVisitor
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.ListQuery
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.execution.command.CreateViewCommand
import org.apache.spark.sql.execution.datasources.CreateTable

object Extractors {
  def extractSourceList(node:TreeNode[_]) : Unit = {
    if (node.nodeName == "UnresolvedRelation")
      GlobalMetaInfo.sourceTableList += TableID.fromArgString(node.argString)
  }
  
  def extractColumnSource(info: ColumnInfo)(node: TreeNode[_]) : Unit = {
    node.nodeName match {
      case "UnresolvedAttribute" => info.addSource(new ColumnInfo(ColumnID.fromName(node.asInstanceOf[UnresolvedAttribute].name)))
      case _ =>
    }
  }
  
  def extractLocal(info: QueryUnitInfo)(node: TreeNode[_]) : Unit = {
    node.nodeName match {
      case "Project"|"Aggregate" => {
        val list = node.nodeName match {
          case "Project" => node.asInstanceOf[Project].projectList
          case "Aggregate" => node.asInstanceOf[Aggregate].aggregateExpressions
        }
        list.map(col => {
          col.nodeName match {
            case "UnresolvedStar" => {
              info.addColumn(new ColumnInfo(ColumnID.fromName(col.asInstanceOf[UnresolvedStar].target.getOrElse(Seq("")).toList.mkString(".") + ".*")))
            }
            case _ => {
              val colInfo = new ColumnInfo(ColumnID.fromName(col.name))
              info.addColumn(colInfo)
              col.children.map(child => LogicalPlanVisitor.visit(child, extractColumnSource(colInfo)(_), LocalNodeStopList))
            }
          }
        })
        GlobalMetaInfo.setProjectVisited(node)
      }
      case "ListQuery" => info.addSource(GlobalMetaInfo.queryUnitInfo(node, "Project").id, false)
      case "Union" | "Except" => info.addSource(GlobalMetaInfo.queryUnitInfo(node, "Merge").id, true)
      case "UnresolvedRelation" => info.addSource(TableID.fromArgString(node.argString), true)
      case "SubqueryAlias" => info.addSource(new TableID(node.asInstanceOf[SubqueryAlias].name), true)
      case _ =>
    }
  }
  
  def extractMergeLocal(info: QueryUnitInfo)(node: TreeNode[_]) : Unit = {
    node.nodeName match {
      case "Project" | "Aggregate" => {
        info.addSource(GlobalMetaInfo.queryUnitInfo(node, "Project").id, true)
      }
      case "ListQuery" => info.addSource(GlobalMetaInfo.queryUnitInfo(node, "Project").id, false)
      case "Union" | "Except" => info.addSource(GlobalMetaInfo.queryUnitInfo(node, "Merge").id, true)
      case "UnresolvedRelation" => info.addSource(TableID.fromArgString(node.argString), true)
      case "SubqueryAlias" => info.addSource(new TableID(node.asInstanceOf[SubqueryAlias].name), true)
      case _ =>
    }
  }
  
  val LocalNodeStopList = Seq("SubqueryAlias", "ListQuery", "UnresolvedRelation", "Union", "Except")
  
  def extractTableLineage(node: TreeNode[_]) : Unit = {
    node.nodeName match {
      case "CreateTable" => {
        val create = node.asInstanceOf[CreateTable]
        val info = GlobalMetaInfo.queryUnitInfo(new TableID(create.tableDesc.identifier), TableLifeType.Table, create)
        create.children.map(child => LogicalPlanVisitor.visit(child, extractLocal(info)(_), LocalNodeStopList))
      }
      case "CreateViewCommand" => {
        val create = node.asInstanceOf[CreateViewCommand]
        val info = GlobalMetaInfo.queryUnitInfo(new TableID(create.name), TableLifeType.TempView, create)
        LogicalPlanVisitor.visit(create.child, extractLocal(info)(_), LocalNodeStopList)
      }
      case "InsertIntoTable" => {
        val insert = node.asInstanceOf[InsertIntoTable]
        val info = GlobalMetaInfo.queryUnitInfo(TableID.fromArgString(insert.table.asInstanceOf[UnresolvedRelation].argString), TableLifeType.Table, insert)
        insert.children.map(child => LogicalPlanVisitor.visit(child, extractLocal(info)(_), LocalNodeStopList))
      }
      case "SubqueryAlias" => {
        val subquery = node.asInstanceOf[SubqueryAlias]
        val info = GlobalMetaInfo.queryUnitInfo(new TableID(subquery.name), TableLifeType.SubQueryAias, subquery)
        LogicalPlanVisitor.visit(subquery.child, extractLocal(info)(_), LocalNodeStopList)
      }
      case "ListQuery" => {
        val listquery = node.asInstanceOf[ListQuery]
        val info = GlobalMetaInfo.queryUnitInfo(node, "Project")
        LogicalPlanVisitor.visit(listquery.plan, extractLocal(info)(_), LocalNodeStopList)
      }
      case "Union" | "Except" => {
        val info = GlobalMetaInfo.queryUnitInfo(node, "Merge")
        node.asInstanceOf[LogicalPlan].children.map(child => LogicalPlanVisitor.visit(child, extractMergeLocal(info)(_), LocalNodeStopList :+ "Project" :+ "Aggregate"))
      }
      case "Project" | "Aggregate" => {
        if (!GlobalMetaInfo.checkProjectVisited(node)) {
          val info = GlobalMetaInfo.queryUnitInfo(node, "Project")
          //project.children.map(x => LogicalPlanVisitor.visit(x, extractLocalSourceTable(info)(_), LocalNodeStopList))
          LogicalPlanVisitor.visit(node, extractLocal(info)(_), LocalNodeStopList)
        }
      }
      case _ =>
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
