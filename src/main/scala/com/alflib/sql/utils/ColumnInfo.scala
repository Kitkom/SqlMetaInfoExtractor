package com.alflib.sql.utils

import scala.collection.mutable.ListBuffer

class ColumnInfo(var id: ColumnID) {
  val sourceList = ListBuffer[ColumnID]()
  def addSource(column:ColumnID): ColumnInfo = {
    sourceList += column
    this
  }
  override def toString() = {
    s"\n$id sources: ${sourceList.mkString(", ")}"
  }
}
