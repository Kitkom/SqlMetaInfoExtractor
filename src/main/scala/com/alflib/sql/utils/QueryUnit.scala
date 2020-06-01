package com.alflib.sql.utils

import com.alflib.sql.exception.ExtractorErrorException
import org.apache.spark.sql.catalyst.trees.TreeNode

import scala.collection.mutable.{ListBuffer, Map}

abstract class QueryUnitInfo(val id: TableID, var lifeType: TableLifeType.Value, var node : TreeNode[_] = null) {
  var directSources = ListBuffer[TableID]()
  var sources = Map[TableID, QueryUnitInfo]()
  def addSource(info: QueryUnitInfo, isDirect: Boolean)
  override def toString() = {
    s"""
       | ========QueryUnitInfo=========
       | ${getClass.getSimpleName} [$id]
       | lifeType     = ${lifeType.toString}
       | directSources= ${directSources})
       | sources      = ${sources.keys})
       | columns      = ${columns}\n
       | ==============================
    """.stripMargin
  }
  // fact source resolving
  var lineageResolved = false
  val sourceTableList = ListBuffer[TableID]()
  
  def resolve() : Unit
  def getSourceTables() : ListBuffer[TableID] = {
    lifeType match {
      case TableLifeType.Table => ListBuffer(id)
      case _ => {
        resolve()
        sourceTableList
      }
    }
  }
  
  val columns = ListBuffer[ColumnInfo]()
  def addColumn(col: ColumnInfo) = {
    columns += col
  }
  def getColumns() : ListBuffer[String]
}

class ProjectUnitInfo(id: TableID, val lifetype: TableLifeType.Value, node : TreeNode[_] = null)
  extends QueryUnitInfo(id, lifetype, node) {
  def addSource(info: QueryUnitInfo, isDirect: Boolean) : Unit = {
    if (this != info) {
      if (isDirect)
        directSources += info.id
      sources(info.id) = info
    }
  }
  def resolve() : Unit = {
    if (!lineageResolved) {
      // table lineage
      lineageResolved = true
      sources.map { case (name, info) =>
        if (info == null)
          sourceTableList += name
        else info.getSourceTables.map(x => sourceTableList += x)
      }
      
      // columns
      // expand stars
      val implicitColumns = ListBuffer[ColumnID]()
      columns.filter(x=>x.id.column == "*").map(col => {
        //val list = if (col.id.table == None) directSources.toList else List(col.id.table.get)
        (if (col.id.table == None) directSources.toList else List(col.id.table.get)).map(srcTable => {
          sources(srcTable).getColumns().map(x=>implicitColumns+=new ColumnID(srcTable, x))
        })
      })
      implicitColumns.map(x=>columns+=new ColumnInfo(ColumnID.fromName(x.column)).addSource(x))
      columns.filter(x=>x.id.column != "*").map(tgtCol=> {
        //if (tgtCol.sourceList.isEmpty) // single select
        //  tgtCol.sourceList += tgtCol.id
        tgtCol.sourceList.map( srcCol =>{
          if (srcCol.table == None) {   // search for source
            if (directSources.size == 1) {  // single source
              srcCol.table = Option(directSources(0))
            }
            else {  // multiple source
              val possibleSources = directSources.map(sources(_)).filter(info => info.getColumns.contains(srcCol.column))
              if (possibleSources.size == 0)
                throw ExtractorErrorException(s"[$id]: Column '${srcCol.column}' not found in sources")
              else
                srcCol.table = Option(possibleSources(0).id)
            }
          }
          else if (!directSources.contains(srcCol.table.get)) {
            throw ExtractorErrorException(s"[$id]: $srcCol not from direct source list")
          }
        })
      })
      // direct alias
      if (lifeType == TableLifeType.DirectAlias) {
        val src = directSources(0)
        sources(src).getColumns().map(x => columns += (new ColumnInfo(ColumnID.fromName(x))).addSource(new ColumnID(src, x)))
      }
  
    }
  }
  def getColumns() : ListBuffer[String] = {
    lifeType match {
      case TableLifeType.Table => getSchema.getColumns
      case _ => {
        resolve()
        columns.map(x => x.id.column)
      }
    }
  }
  def getSchema() : TableSchema = {
    lifeType match {
      case TableLifeType.Table => CommonUtils.getTableSchema(id)
      case _ => null
    }
  }
}

class MergeUnitInfo(val number: Int, val mergeType: String, node : TreeNode[_])
  extends QueryUnitInfo(new TableID(s"__merge__${mergeType}__", number.toString), TableLifeType.Local, node) {
  def addSource(info: QueryUnitInfo, isDirect: Boolean) : Unit = {
    if (this != info) {
      if (sources.size < 2) {
        sources(info.id) = info
        directSources += info.id
      }
      else
        throw ExtractorErrorException(s"MergeUnit ${id} has more than 2 sources.")
    }
  }
  def resolve() = {
    if (!lineageResolved) {
      lineageResolved = true
      sources(directSources(0)).getColumns().map(x => columns += new ColumnInfo(ColumnID.fromName(x)))
      for (index <- 0 to (columns.size - 1))
        columns(index)
          .addSource(new ColumnID(directSources(0), sources(directSources(0)).getColumns()(index)))
          .addSource(new ColumnID(directSources(1), sources(directSources(1)).getColumns()(index)))
    }
  }
  def getColumns() : ListBuffer[String] = {
    resolve()
    columns.map(_.id.column)
  }
  
}

