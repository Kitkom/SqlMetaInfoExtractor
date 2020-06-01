package com.alflib.sql.utils

import com.alflib.sql.exception.ExtractorErrorException
import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, SubqueryAlias}
import org.apache.spark.sql.execution.command.CreateViewCommand
import com.alflib.sql.utils.TableLifeType.{Local, Table, Unknown}
import org.apache.spark.sql.catalyst.IdentifierWithDatabase
import org.apache.spark.sql.catalyst.trees.TreeNode

import scala.collection.mutable.{ListBuffer, Map}

object GlobalMetaInfo {
  
  val logger: Logger=Logger.getLogger(getClass)
  
  var sourceTableList = ListBuffer[TableID]()
  val queryUnitInfoList = ListBuffer[QueryUnitInfo]()
  var idToQueryUnitInfoMap = Map[TableID, QueryUnitInfo]()
  var nodeToQueryUnitInfoMap = Map[TreeNode[_], QueryUnitInfo]()
  var visitedProject = Map[TreeNode[_], Boolean]()
  var errors = Map[String, Exception]()
  
  def queryUnitInfo(name: String) : QueryUnitInfo = queryUnitInfo(TableID.fromArgString(name))
  def queryUnitInfo(id: IdentifierWithDatabase): QueryUnitInfo = queryUnitInfo(TableID.fromID(id))
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
  
  def checkProjectVisited(node: TreeNode[_]) = visitedProject.contains(node)
  
  def setNodeVisited(node: TreeNode[_]) = {
    logger.debug("set visited")
    /*
    node.nodeName match {
      case "Project"|"Aggregate"=>
      case _ => throw ExtractorErrorException(s"${node.nodeName} not projection.")
    }
    */
    visitedProject(node) = true
  }
  
  def getQueryUnitInfo(tblID: TableID) = {
    idToQueryUnitInfoMap.getOrElse(tblID, null)
  }
  
  def queryUnitInfo(node: TreeNode[_], unitType: String) : QueryUnitInfo = {
    if (!nodeToQueryUnitInfoMap.contains(node)) {
      val info = unitType match {
        case "Merge" => new MergeUnitInfo (queryUnitInfoList.size, node.nodeName, node)
        case "Project" => new ProjectUnitInfo (new TableID (None, "__anonymous__" + queryUnitInfoList.size.toString), Local, node)
        case _ => null
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
    queryUnitInfoList.map(_.resolve)
  }
  
  def clear = {
    sourceTableList.clear
    idToQueryUnitInfoMap.clear
    nodeToQueryUnitInfoMap.clear
    queryUnitInfoList.clear
    errors.clear
  }
  
  def cleanUp = {
    logger.debug(queryUnitInfoList)
    logger.debug("====================================================================================")
    logger.debug("==================================AFTER RESOLVING===================================")
    logger.debug("====================================================================================")
    try {
      resolveQueryUnits()
    }
    catch {
      case e : ExtractorErrorException => errors(s"__on_clean_up__${errors.size}") = e
    }
    logger.debug(sourceTableList)
    //logger.debug(idToQueryUnitInfoMap.values)
    logger.debug(queryUnitInfoList)
    if (!errors.isEmpty) logger.error(errors)
  }
  
  def getSources = {
    val sources = ListBuffer[TableID]()
    /*
    queryUnitInfoList.map(info => info.getSourceTables().map(x=>sources+=x))
    sources.filterNot(x=>idToQueryUnitInfoMap.contains(x)).toList.distinct
    */
    queryUnitInfoList.filter(_.lifeType==TableLifeType.Table).map(info => info.id)
  }
}
