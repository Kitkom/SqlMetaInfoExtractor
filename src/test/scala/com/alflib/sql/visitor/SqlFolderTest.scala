package com.alflib.sql.visitor

import java.io.File

import com.alflib.sql.utils.{CommonUtils, Extractors, GlobalMetaInfo}
import org.apache.log4j.{Level, Logger}

import scala.io.Source

object SqlFolderTest {
  
  val logger: Logger=Logger.getLogger(getClass)
  
  def main(args:Array[String]) : Unit = {
    if (args.size == 0) return
    val dirname = args(0)
    val filelist = new File(dirname).listFiles().filterNot(_.isDirectory).map(_.getPath)
    filelist.map(file => {
      val sql = new StringBuilder
      for (line<-Source.fromFile(file, "utf-8").getLines)
        sql.append(s"$line\n")
      GlobalMetaInfo.clear
      CommonUtils.visitMultipleSqls(sql.toString, x => LogicalPlanVisitor.visit(x, Extractors.extractMetaInfo(_)))
      GlobalMetaInfo.cleanUp
      logger.info(s"File ${file}, source list:")
      logger.info(GlobalMetaInfo.getSources)
    })
  }
  
}

object SqlFileTest {
  
  val logger: Logger=Logger.getLogger(getClass)
  
  def main(args:Array[String]) : Unit = {
    Logger.getRootLogger.setLevel(Level.DEBUG)
    if (args.size == 0) return
    val file = args(0)
    logger.info(s"Parsing file ${file}")
    
    val sql = new StringBuilder
    for (line<-Source.fromFile(file, "utf-8").getLines)
      sql.append(line)
    GlobalMetaInfo.clear
    CommonUtils.visitMultipleSqls(sql.toString, x => LogicalPlanVisitor.visit(x, Extractors.extractMetaInfo(_)))
    GlobalMetaInfo.cleanUp
    logger.info(s"File ${file}, source list:")
    logger.info(GlobalMetaInfo.getSources)
  }
  
}
