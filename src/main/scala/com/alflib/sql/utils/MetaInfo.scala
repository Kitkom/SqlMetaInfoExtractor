package com.alflib.sql.utils

import com.alflib.sql.exception.ExtractorErrorException
import org.apache.spark.sql.catalyst.IdentifierWithDatabase
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.trees.TreeNode

import scala.collection.mutable.{ListBuffer, Map}

object TableLifeType extends Enumeration {
  val Table, TempView, SubQueryAlias, DirectAlias, Local, Unknown = Value
}

object TableID {
  def fromArgString(src: String) = {
    val list = src.replaceAll("`", "").split("\\.")
    if (list.size == 1)
      TableID(None, list(0))
    else if (list.size == 2)
      new TableID(list(0), list(1))
    else
      throw ExtractorErrorException(s"${src} is not a valid table/view identifier!")
  }
}

case class TableID(var database: Option[String] = None, var table: String) {
  def this(database: String, table: String) = this(Option(database), table)
  def this(src: IdentifierWithDatabase) = this(src.database, src.identifier)
  override def toString() = if (database == None) table else s"${database.get}.$table"
}

object ColumnID {
  def fromName(name: String) = {
    val nameParts = name.split("\\.")
    nameParts.size match {
      case 1 => ColumnID(None, nameParts(0))
      case 2 => new ColumnID(new TableID(None, nameParts(0)), nameParts(1))
      case _ => new ColumnID(new TableID(nameParts(0), nameParts(1)), nameParts(2))
    }
  }
}

case class ColumnID (var table: Option[TableID], val column: String) {
  def this(tbl: TableID, column: String) = this(Option(tbl), column)
  def isResolved() = table==None
  def setTable(table:TableID) = {this.table=Option(table)}
  override def toString() = if (table == None) column else s"${table.get}.$column"
}

/*
class QueryUnitInfo(val id: TableID, var lifeType: TableLifeType.Value, var node : TreeNode[_] = null) {
  var sources = Map[TableID, QueryUnitInfo]()
  def addSource(tblId: TableID) : Unit = {if (id != tblId) sources(tblId) = GlobalMetaInfo.getQueryUnitInfo(tblId)}
  override def toString() = {
    s"""
      | ========QueryUnitInfo=========
      | [$id]
      | lifeType     = ${lifeType.toString}
      | sources      = (${sources.keys})
      |    ========node=========
      |    ${node}
      |    ========node=========
      | ==============================
    """.stripMargin
  }
  
  //| sourceTables = (${getSourceTables()})
  
  val sourceTableList = ListBuffer[TableID]()
  var lineageResolved = false
  
  def getSourceTables() : ListBuffer[TableID] = {
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
    sourceTableList
  }
  
}
*/
