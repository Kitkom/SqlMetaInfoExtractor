package com.alflib.sql.utils

import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, SubqueryAlias}
import org.apache.spark.sql.execution.command.CreateViewCommand
import com.alflib.sql.utils.TableLifeType.{Local, Table, Unknown}
import org.apache.spark.sql.catalyst.trees.TreeNode

import scala.collection.mutable.{ListBuffer, Map}

object GlobalMetaInfo {
  
  val logger: Logger=Logger.getLogger(getClass)
  
  var sourceTableList = ListBuffer[TableID]()
  val queryUnitInfoList = ListBuffer[QueryUnitInfo]()
  var idToQueryUnitInfoMap = Map[TableID, QueryUnitInfo]()
  var nodeToQueryUnitInfoMap = Map[TreeNode[_], QueryUnitInfo]()
  var visitedProject = Map[Project, Boolean]()
  var errors = Map[String, Exception]()
  
  
  def queryUnitInfo(tblID: TableID, lifeType: TableLifeType.Value = Unknown, node: TreeNode[_] = null) = {
    if (!idToQueryUnitInfoMap.contains(tblID)) {
      val info = new ProjectUnitInfo(tblID, lifeType)
      logger.debug(s"new unit named ${tblID}")
      queryUnitInfoList += info
      idToQueryUnitInfoMap(tblID) = info
    }
    val info = idToQueryUnitInfoMap(tblID)
    if (info.lifeType == Unknown)
      info.lifeType = lifeType
    if (info.node == null) {
      info.node = node
      nodeToQueryUnitInfoMap(node) = info
    }
    info
  }
  
  def checkProjectVisited(node: Project) = visitedProject.contains(node)
  
  def setProjectVisited(node: Project) = {
    logger.debug("set visited")
    visitedProject(node) = true
  }
  
  def getQueryUnitInfo(tblID: TableID) = {
    idToQueryUnitInfoMap.getOrElse(tblID, null)
  }
  
  def queryUnitInfo(node: TreeNode[_], unitType: String) : QueryUnitInfo = {
    if (!nodeToQueryUnitInfoMap.contains(node)) {
      val info = unitType match {
        case "Merge" => new MergeUnitInfo (queryUnitInfoList.size, node.nodeName, node)
        case _ => new ProjectUnitInfo (new TableID (None, "__anonymous__" + queryUnitInfoList.size.toString), Local, node)
      }
      logger.debug(s"new unit named ${info.id}")
      queryUnitInfoList += info
      nodeToQueryUnitInfoMap(node) = info
      idToQueryUnitInfoMap(info.id) = info
    }
    nodeToQueryUnitInfoMap(node)
  }
  
  def resolveQueryUnits() = {
    // query external database
    // <TBD>
    
    // set unkown units to Table
    idToQueryUnitInfoMap.values.map(x => if (x.lifeType==Unknown) x.lifeType = Table)
    
    // resolve * columns
  }
  
  def clear = {
    sourceTableList.clear
    idToQueryUnitInfoMap.clear
    nodeToQueryUnitInfoMap.clear
    queryUnitInfoList.clear
    errors.clear
  }
  
  def cleanUp = {
    resolveQueryUnits()
    logger.debug(sourceTableList)
    //logger.debug(idToQueryUnitInfoMap.values)
    logger.debug(queryUnitInfoList)
    if (!errors.isEmpty) logger.error(errors)
  }
  
  def getSources = {
    val sources = ListBuffer[TableID]()
    queryUnitInfoList.map(info=> info.getSourceTables().map(x=>sources+=x))
    sources.filterNot(x=>idToQueryUnitInfoMap.contains(x)).toList.distinct
  }
}
