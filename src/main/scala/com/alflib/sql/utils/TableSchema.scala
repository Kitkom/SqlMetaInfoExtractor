package com.alflib.sql.utils

import scala.collection.mutable.ListBuffer

class TableSchema(val id: TableID) {
  val columns = ListBuffer[String]()
  def addColumn(col: String) = columns += col
  def getColumns() = columns
}
