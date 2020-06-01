package com.alflib.sql.utils

import com.alflib.sql.visitor.LogicalPlanVisitor
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAlias, UnresolvedAttribute, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, ListQuery}
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
      case "UnresolvedAttribute" => info.addSource(ColumnID.fromName(node.asInstanceOf[UnresolvedAttribute].name))
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
            case "UnresolvedStar" => info.addColumn(new ColumnInfo(ColumnID.fromName(col.asInstanceOf[UnresolvedStar].target.getOrElse(Seq()).toList.map(_+".").mkString("") + "*")))
            case _ => {
              val name = if (("UnresolvedAlias").contains(col.nodeName) && (!("Alias").contains(col.nodeName)))"__anonymous_column__" else col.name
              val colID = ColumnID.fromName(name)
              val colInfo = new ColumnInfo(ColumnID.fromName(colID.column))
              info.addColumn(colInfo)
              if (col.children.isEmpty)
                colInfo.addSource(colID)
              else
                col.children.map(child => LogicalPlanVisitor.visit(child, extractColumnSource(colInfo)(_), LocalNodeStopList))
            }
          }
        })
        GlobalMetaInfo.setNodeVisited(node)
      }
      case "Generate" => {
        val generate = node.asInstanceOf[Generate]
        if (!GlobalMetaInfo.checkProjectVisited(generate)) {
          GlobalMetaInfo.setNodeVisited(generate)
          val gInfo = GlobalMetaInfo.queryUnitInfo(generate, "Project")
          info.addSource(gInfo, true)
          generate.generatorOutput.map(x => {
            val colInfo = new ColumnInfo(ColumnID.fromName(x.name))
            LogicalPlanVisitor.visit(generate.generator, extractColumnSource(colInfo)(_), LocalNodeStopList)
            gInfo.addColumn(colInfo)
          })
        }
      }
      case "ListQuery" => info.addSource(GlobalMetaInfo.queryUnitInfo(node, "Project"), false)
      case "Union" | "Except" => info.addSource(GlobalMetaInfo.queryUnitInfo(node, "Merge"), true)
      case "UnresolvedRelation" => info.addSource(GlobalMetaInfo.queryUnitInfo(node.argString), true)
      case "SubqueryAlias" => info.addSource(GlobalMetaInfo.queryUnitInfo(node.asInstanceOf[SubqueryAlias].name), true)
      case _ =>
    }
  }
  
  def extractMergeLocal(info: QueryUnitInfo)(node: TreeNode[_]) : Unit = {
    node.nodeName match {
      case "Project" | "Aggregate" => {
        info.addSource(GlobalMetaInfo.queryUnitInfo(node, "Project"), true)
      }
      case "ListQuery" => info.addSource(GlobalMetaInfo.queryUnitInfo(node, "Project"), false)
      case "Union" | "Except" => info.addSource(GlobalMetaInfo.queryUnitInfo(node, "Merge"), true)
      case "UnresolvedRelation" => info.addSource(GlobalMetaInfo.queryUnitInfo(node.argString), true)
      case "SubqueryAlias" => info.addSource(GlobalMetaInfo.queryUnitInfo(node.asInstanceOf[SubqueryAlias].name), true)
      case _ =>
    }
  }
  
  val LocalNodeStopList = Seq("SubqueryAlias", "ListQuery", "UnresolvedRelation", "Union", "Except")
  
  def extractTableLineage(node: TreeNode[_]) : Unit = {
    node.nodeName match {
      case "CreateTable" => {
        val create = node.asInstanceOf[CreateTable]
        val info = GlobalMetaInfo.queryUnitInfo(TableID.fromID(create.tableDesc.identifier), TableLifeType.Table, create)
        create.children.map(child => LogicalPlanVisitor.visit(child, extractLocal(info)(_), LocalNodeStopList))
      }
      case "CreateViewCommand" => {
        val create = node.asInstanceOf[CreateViewCommand]
        val info = GlobalMetaInfo.queryUnitInfo(TableID.fromID(create.name), TableLifeType.TempView, create)
        LogicalPlanVisitor.visit(create.child, extractLocal(info)(_), LocalNodeStopList)
      }
      case "InsertIntoTable" => {
        val insert = node.asInstanceOf[InsertIntoTable]
        val info = GlobalMetaInfo.queryUnitInfo(TableID.fromArgString(insert.table.asInstanceOf[UnresolvedRelation].argString), TableLifeType.Table, insert)
        insert.children.map(child => LogicalPlanVisitor.visit(child, extractLocal(info)(_), LocalNodeStopList))
      }
      case "SubqueryAlias" => {
        val subquery = node.asInstanceOf[SubqueryAlias]
        val info = GlobalMetaInfo.queryUnitInfo(
          TableID.fromID(subquery.name),
          if (subquery.child.nodeName == "UnresolvedRelation")  TableLifeType.DirectAlias else TableLifeType.SubQueryAlias ,
          subquery
        )
        LogicalPlanVisitor.visit(subquery.child, extractLocal(info)(_), LocalNodeStopList)
      }
      case "ListQuery" => {
        val listquery = node.asInstanceOf[ListQuery]
        val info = GlobalMetaInfo.queryUnitInfo(node, "Project")
        LogicalPlanVisitor.visit(listquery.plan, extractLocal(info)(_), LocalNodeStopList)
      }
      case "Generate" => {
        val generate = node.asInstanceOf[Generate]
        val info = GlobalMetaInfo.queryUnitInfo(generate, "Project")
        LogicalPlanVisitor.visit(generate.child, extractLocal(info)(_), LocalNodeStopList)
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
