package com.alflib.sql.visitor

import java.io.File

import com.alflib.sql.utils.{CommonUtils, Extractors, GlobalMetaInfo, TableLifeType}
import org.apache.log4j.{Level, Logger}

import scala.io.Source

object SqlFolderTest {
  
  val logger: Logger=Logger.getLogger(getClass)
  
  def main(args:Array[String]) : Unit = {
    if (args.size == 0) return
    val dirname = args(0)
    val filelist = new File(dirname).listFiles().filterNot(_.isDirectory).map(_.getPath)
    filelist.map(file => {
      GlobalMetaInfo.clear
      CommonUtils.visitFile(file, x => LogicalPlanVisitor.visit(x, Extractors.extractMetaInfo(_)))
      GlobalMetaInfo.cleanUp
      logger.info(s"File ${file}, source list:")
      logger.info(GlobalMetaInfo.getSources)
      GlobalMetaInfo.queryUnitInfoList.filter(_.lifeType == TableLifeType.External)
        .map(unit => CommonUtils.getAllColumnLineage(unit , (t, s) => logger.info(s"${t.toString} <= ${s.toString}")))
      if (!GlobalMetaInfo.errors.isEmpty)
        logger.error(GlobalMetaInfo.errors.values)
    })
  }
  
}

object SqlFileTest {
  
  val logger: Logger=Logger.getLogger(getClass)
  
  def main(args:Array[String]) : Unit = {
    Logger.getRootLogger.setLevel(Level.DEBUG)
    if (args.size == 0) return
    val file = args(0)
    
    GlobalMetaInfo.clear
    CommonUtils.visitFile(file, x => LogicalPlanVisitor.visit(x, Extractors.extractMetaInfo(_)))
    GlobalMetaInfo.cleanUp
    logger.info(s"File ${file}, source list:")
    logger.info(GlobalMetaInfo.getSources)
    GlobalMetaInfo.queryUnitInfoList.filter(_.lifeType == TableLifeType.External)
      .map(unit => CommonUtils.getAllColumnLineage(unit , (t, s) => logger.info(s"${t.toString} <= ${s.toString}")))
  }
  
}
