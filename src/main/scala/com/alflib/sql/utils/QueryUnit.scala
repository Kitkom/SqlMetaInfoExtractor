package com.alflib.sql.utils

import com.alflib.sql.exception.ExtractorErrorException
import org.apache.spark.sql.catalyst.trees.TreeNode

import scala.collection.mutable.{ListBuffer, Map}

abstract class QueryUnitInfo(val id: TableID, var lifeType: TableLifeType.Value, var node : TreeNode[_] = null) {
  var sources = Map[TableID, QueryUnitInfo]()
  def addSource(tblId: TableID)
  override def toString() = {
    s"""
       | ========QueryUnitInfo=========
       | ${getClass.getSimpleName} [$id]
       | lifeType     = ${lifeType.toString}
       | sources      = (${sources.keys})
       | ==============================
    """.stripMargin
  }
  // fact source resolving
  var lineageResolved = false
  val sourceTableList = ListBuffer[TableID]()
  def resolve() : Unit = {
    if (!lineageResolved) {
      lineageResolved = true
      sources.map { case (name, info) =>
        if (info == null)
          sourceTableList += name
        else {
          info.lifeType match {
            case TableLifeType.Table => sourceTableList += name
            case _ => info.getSourceTables.map(x => sourceTableList += x)
          }
        }
      }
    }
  }
  def getSourceTables() : ListBuffer[TableID] = {
    resolve()
    sourceTableList
  }
}

class ProjectUnitInfo(id: TableID, lifeType: TableLifeType.Value, node : TreeNode[_] = null)
  extends QueryUnitInfo(id, lifeType, node) {
  def addSource(tblId: TableID) : Unit = {
    if (id != tblId)
      sources(tblId) = GlobalMetaInfo.getQueryUnitInfo(tblId)
  }
}

class MergeUnitInfo(val number: Int, val mergeType: String, node : TreeNode[_])
  extends QueryUnitInfo(new TableID(s"__merge__${mergeType}__", number.toString), TableLifeType.Local, node) {
  def addSource(tblId: TableID) : Unit = {
    if (id != tblId) {
      if (sources.size < 2)
        sources(tblId) = GlobalMetaInfo.getQueryUnitInfo(tblId)
      else
        throw ExtractorErrorException(s"MergeUnit ${id} has more than 2 sources.")
    }
  }
}
