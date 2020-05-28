package com.alflib.sql.utils

import scala.collection.mutable.ListBuffer

class ColumnInfo(val id: ColumnID) {
  val sourceList = ListBuffer[ColumnInfo]()
  def addSource(column:ColumnInfo): Unit = {
    if (!sourceList.contains(column)) sourceList += column
  }
  override def toString() = {
    s"\n$id sources: ${sourceList.map(x=>x.id).mkString(", ")}"
  }
}
