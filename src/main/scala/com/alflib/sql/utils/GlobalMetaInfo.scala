package com.alflib.sql.utils

import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.execution.command.CreateViewCommand

import scala.collection.mutable.{ListBuffer, Map}

object GlobalMetaInfo {
  
  val logger: Logger=Logger.getLogger(getClass)
  
  var sourceTableList = ListBuffer[String]()
  var tempViewMap = Map[String, LogicalPlan]()
  
  def clear : Unit = {
    sourceTableList.clear
    tempViewMap.clear
  }
  
  def cleanUp : Unit = {
    logger.debug(sourceTableList)
    logger.debug(tempViewMap)
    sourceTableList = sourceTableList.filter((id:String) => !tempViewMap.contains(id))
  }
  
  def extractSourceList(node:LogicalPlan) : Unit = {
    if (node.nodeName == "UnresolvedRelation") {
      sourceTableList += CommonUtils.getIdentifier(node.argString)
    }
  }
  
  def extractTempView(node: LogicalPlan) : Unit = {
    if (node.nodeName == "SubqueryAlias") {
      val alias = node.asInstanceOf[SubqueryAlias]
      tempViewMap(CommonUtils.getIdentifier(alias.name)) = alias.child
    }
    else if (node.nodeName == "CreateViewCommand") {
      val create = node.asInstanceOf[CreateViewCommand]
      tempViewMap(CommonUtils.getIdentifier(create.name)) = create.child
    }
  }
  
  def extractMetaInfo(node: LogicalPlan) : Unit = {
    extractSourceList(node)
    extractTempView(node)
  }
}
