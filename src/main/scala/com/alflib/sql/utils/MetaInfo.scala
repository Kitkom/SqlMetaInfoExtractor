package com.alflib.sql.utils

case class TableID(val database: Option[String] = None, val table: String) {
  def this(database: String, table: String) = {this(Option(database), table)}
  override def toString() = if (database == None) table else s"${database.get}.$table"
}

case class ColumnID (val table: TableID, val column: String) {
  override def toString() = {s"${table.toString}.$column"}
}


