package com.alflib.sql.utils

import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, SubqueryAlias}
import org.apache.spark.sql.execution.command.CreateViewCommand
import com.alflib.sql.utils.TableLifeType.{Table, Unknown, Local}

import scala.collection.mutable.{ListBuffer, Map}

object GlobalMetaInfo {
  
  val logger: Logger=Logger.getLogger(getClass)
  
  var sourceTableList = ListBuffer[TableID]()
  var targetedQueryUnitInfoMap = Map[TableID, QueryUnitInfo]()
  var untargetedQueryUnitInfoMap = Map[Project, QueryUnitInfo]()
  var errors = Map[String, Exception]()
  
  def getQueryUnitInfo(tblID: TableID, lifeType: TableLifeType.Value = Unknown, logicalPlan: Project = null) = {
    if (!targetedQueryUnitInfoMap.contains(tblID))
      targetedQueryUnitInfoMap(tblID) = new QueryUnitInfo(tblID, lifeType)
    val info = targetedQueryUnitInfoMap(tblID)
    if (info.lifeType == Unknown)
      info.lifeType = lifeType
    if (info.logicalPlan == null)
      info.logicalPlan = logicalPlan
    info
  }
  
  def checkProjectRegistered(plan: Project) = {
    targetedQueryUnitInfoMap.values.map(x => x.logicalPlan).toList.contains(plan) || untargetedQueryUnitInfoMap.keys.toList.contains(plan)
  }
  
  def getQueryUnitInfo(plan: Project) = {
    if (!untargetedQueryUnitInfoMap.contains(plan))
      untargetedQueryUnitInfoMap(plan) = new QueryUnitInfo(new TableID("untargeted", untargetedQueryUnitInfoMap.keys.size.toString), Local, plan)
    untargetedQueryUnitInfoMap(plan)
  }
  
  def resolveQueryUnits() = {
    // query external database
    // <TBD>
    
    // set unkown units to Table
    targetedQueryUnitInfoMap.values.map(x => if (x.lifeType==Unknown) x.lifeType = Table)
    
    // resolve * columns
  }
  
  def clear = {
    sourceTableList.clear
    targetedQueryUnitInfoMap.clear
    untargetedQueryUnitInfoMap.clear
    errors.clear
  }
  
  def cleanUp = {
    resolveQueryUnits()
    logger.debug(sourceTableList)
    logger.debug(targetedQueryUnitInfoMap.values)
    logger.debug(untargetedQueryUnitInfoMap.values)
    if (!errors.isEmpty) logger.error(errors)
  }
  
  def getSources = {
      targetedQueryUnitInfoMap.values.filter(x=>x.lifeType==Table).map(x=>x.id.toString).toList.distinct
  }
}
