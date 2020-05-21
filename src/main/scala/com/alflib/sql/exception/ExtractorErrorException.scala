package com.alflib.sql.exception

case class ExtractorErrorException(val message:String = "", val cause:Throwable = None.orNull) extends Exception(message, cause)
