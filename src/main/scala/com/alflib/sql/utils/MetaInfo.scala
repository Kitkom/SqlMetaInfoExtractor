package com.alflib.sql.utils

import com.alflib.sql.exception.ExtractorErrorException
import org.apache.spark.sql.catalyst.IdentifierWithDatabase
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}

import scala.collection.mutable.{ListBuffer, Map}

object TableLifeType extends Enumeration {
  val Table, TempView, SubQueryAias, Local, Unknown = Value
}

object TableID {
  def convertArgString(src: String) = {
    val list = src.replaceAll("`", "").split("\\.")
    if (list.size == 1)
      TableID(None, list(0))
    else if (list.size == 2)
      new TableID(list(0), list(1))
    else
      throw ExtractorErrorException(s"${src} is not a valid table/view identifier!")
  }
}

case class TableID(val database: Option[String] = None, val table: String) {
  def this(database: String, table: String) = this(Option(database), table)
  def this(src: IdentifierWithDatabase) = this(src.database, src.identifier)
  override def toString() = if (database == None) table else s"${database.get}.$table"
}

case class ColumnID (var table: Option[TableID], val column: String) {
  def this(tbl: TableID, column: String) = this(Option(tbl), column)
  override def toString() = {s"${table.toString}.$column"}
  def isResolved() = table==None
  def setTable(table:TableID) = {this.table=Option(table)}
}

class QueryUnitInfo(val id: TableID, var lifeType: TableLifeType.Value, var logicalPlan : Project = null) {
  var columns = Map[ColumnID, String]()
  var sources = Map[TableID, QueryUnitInfo]()
  def addColumns(cols: List[String]) = cols.map(x=>addColumn(x))
  def addColumn(col: String) = columns(new ColumnID(id, col)) = null
  def addSource(tblInfo: QueryUnitInfo) : Unit = {sources(tblInfo.id) = tblInfo}
  override def toString() = {
    s"""
      | ========QueryUnitInfo=========
      | $id
      | lifeType     = ${lifeType.toString}
      | columns      = (${columns.toList})
      | sources      = (${sources.keys})
      | sourceTables = (${getSourceTables()})
      | ==============================
    """.stripMargin
  }
  
  val sourceTableList = ListBuffer[TableID]()
  var sourceResolved = false
  def getSourceTables() : ListBuffer[TableID] = {
    if (!sourceResolved) {
      sourceResolved = true
      sources.map { case (name, info) => {
        info.lifeType match {
          case TableLifeType.Table => sourceTableList += name
          case _ => {
            if (info.sourceTableList.isEmpty) info.getSourceTables
            info.sourceTableList.map(x => sourceTableList += x)
          }
        }
      }
      }
    }
    sourceTableList
  }
  
}
