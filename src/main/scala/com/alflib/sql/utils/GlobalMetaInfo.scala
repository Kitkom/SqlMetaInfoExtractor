package com.alflib.sql.utils

import com.alflib.sql.exception.ExtractorErrorException
import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, LogicalPlan, Project, SubqueryAlias}
import org.apache.spark.sql.execution.command.CreateViewCommand
import com.alflib.sql.utils.TableLifeType.{External, Local, Unknown}
import org.apache.spark.sql.catalyst.IdentifierWithDatabase
import org.apache.spark.sql.catalyst.analysis.{UnresolvedOrdinal, UnresolvedRelation}
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
  
  def idToQueryUnitInfo(name: String) : QueryUnitInfo = idToQueryUnitInfo(TableID.fromArgString(name))
  def idToQueryUnitInfo(id: IdentifierWithDatabase): QueryUnitInfo = idToQueryUnitInfo(TableID.fromID(id))
  def idToQueryUnitInfo(tblID: TableID, lifeType: TableLifeType.Value = Unknown, node: TreeNode[_] = null) = {
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
  
  def nodeToQueryUnitInfo(node: TreeNode[_], unitType: String) : QueryUnitInfo = {
    if (!nodeToQueryUnitInfoMap.contains(node)) {
      val info = unitType match {
        case "Merge" => new MergeUnitInfo (queryUnitInfoList.size, node.nodeName, node)
        case "Project" => new ProjectUnitInfo (new TableID (None, "__anonymous__" + queryUnitInfoList.size.toString), Local, node)
        case "Insert" => {
          val id = node.asInstanceOf[InsertIntoTable].table.asInstanceOf[UnresolvedRelation].tableIdentifier
          new ProjectUnitInfo(TableID.fromID(id), External, node)
        }
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
    
    // resolve null sources
    queryUnitInfoList.map(x=>x.sources.filter{case(k,v)=>v==null}.keys.map(id=>x.sources(id)=idToQueryUnitInfo(id)))
    
    // set unkown units to Table
    idToQueryUnitInfoMap.values.map(x => if (x.lifeType==Unknown) x.lifeType = External)
    
    // resolve * columns
    queryUnitInfoList.map(_.resolve(s=>errors("on_resolving_"+errors.size) = new ExtractorErrorException(s)))
  }
  
  def clear = {
    sourceTableList.clear
    idToQueryUnitInfoMap.clear
    nodeToQueryUnitInfoMap.clear
    queryUnitInfoList.clear
    errors.clear
  }
  
  def cleanUp = {
    logger.debug(idToQueryUnitInfoMap.keys)
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
    errors.map(logger.error(_))
  }
  
  def getSources = {
    val sources = ListBuffer[TableID]()
    /*
    queryUnitInfoList.map(info => info.getSourceTables().map(x=>sources+=x))
    sources.filterNot(x=>idToQueryUnitInfoMap.contains(x)).toList.distinct
    */
    queryUnitInfoList.filter(_.lifeType==TableLifeType.External).map(info => info.id)
  }
}
