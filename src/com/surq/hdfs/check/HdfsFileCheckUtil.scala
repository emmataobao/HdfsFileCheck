package com.surq.hdfs.check

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileStatus
import java.io.File
import java.io.FileInputStream
import java.text.SimpleDateFormat
import scala.collection.mutable.ArrayBuffer

/**
 * @author 宿荣全
 * @date 2018-12-07
 * @com spark程序起动时，需要检查文件是否存在，要生成的路径要删除
 */
class HdfsFileCheckUtil(toAddressList: Array[String]) {

  // HDFS 文件配置
  val configPath = LoadFilePath.getConfigPath
  val conf = new Configuration
  conf.addResource(new Path(configPath + "core-site.xml"))
  conf.addResource(new Path(configPath + "hdfs-site.xml"))
  //否则报：No FileSystem for scheme: hdfs
  conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
  val HDFSFileSytem = FileSystem.get(conf)
  val mesgList = ArrayBuffer[(String, String)]()

  val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  /**
   * 发送HDFS文件监控 异常内容
   */
  def sendEmail = if (mesgList.size > 0) sendListMsg
  /**
   * 判断文件夹下文件个数是否= count
   */
  def isFileCount(path: String, count: Int) = {
    val fileSize = HDFSFileSytem.listStatus(new Path(path)).size
    if (fileSize == count) true else {
      mesgList += (dateFormat.format(System.currentTimeMillis) + ":文件个数判断" -> (path + "下有" + fileSize + "个，不等于预期的" + count + "个。"))
    }
  }
  def isFilesCount(pathList: Array[(String, Int)]) = {
    val resultList = pathList.map(path => (HDFSFileSytem.listStatus(new Path(path._1)).size, path._2, path._1)).filter(kv => kv._1 != kv._2)
    if (resultList.size > 0) {
      resultList.map(line => mesgList += (dateFormat.format(System.currentTimeMillis) + ":文件个数判断" -> (line._3 + "下有" + line._1 + "个，不等于预期的" + line._2 + "个。")))
      false
    } else true
  }

  /**
   * 直到指定的文件存在为止
   */
  def waitFileExist(path: String, waitTime: Int = 1) = {
    val hdfsPath = new Path(path)
    var flg = true
    while (!isFileExist(hdfsPath)) {
      if (flg) {
        mesgList += (dateFormat.format(System.currentTimeMillis) + ":判断文件是否存在" -> (hdfsPath.toString + "不存在，开始等待。。。"))
        sendListMsg
        flg = false
      }
      Thread.sleep(waitTime * 1000)
    }
    if (!flg) {
      mesgList += (dateFormat.format(System.currentTimeMillis) + ":判断文件是否存在" -> (hdfsPath.toString + "已就绪。"))
      sendListMsg
    }
  }

  /**
   * 直到指定的文件存在为止
   */
  def waitFilesExist(pathList: Array[String], waitTime: Int = 1) = {
    var hdfsPathList = pathList.map(f => new Path(f))
    var flag = true
    while (hdfsPathList.size > 0) {
      // 留下不存在的文件
      hdfsPathList = hdfsPathList.filter(path => !isFileExist(path))
      if (hdfsPathList.size > 0 && flag) {
        hdfsPathList.map(path => mesgList += (dateFormat.format(System.currentTimeMillis) + ":判断文件是否存在" -> (path.toString + "不存在，开始等待。。。")))
        sendListMsg
        flag = false
      }
      Thread.sleep(waitTime * 1000)
    }
    if (!flag) {
      mesgList += (dateFormat.format(System.currentTimeMillis) + ":判断文件是否存在" -> (pathList.mkString(",") + "已全部就绪。"))
      sendListMsg
    }
  }

  /**
   * 发送邮件
   */
  def sendListMsg = {
    if (toAddressList.size > 0) {
      MailUtil.sendHtmlMail(toAddressList, "HDFS文件监控", mesgList.mkString("\t"))
      mesgList.clear
    }
  }
  /**
   * 文件夹是否包涵指定的文件
   */
  def isContainFile(path: String, fileName: String) = {
    val hdfspath = new Path(path)
    val status = HDFSFileSytem.listStatus(new Path(path))
    val list = status.map(fileStatus => {
      val path = fileStatus.getPath.toString
      path.substring(path.lastIndexOf(System.getProperty("file.separator")) + 1)
    })
    val flg = list.contains(fileName)
    if (!flg) mesgList += (dateFormat.format(System.currentTimeMillis) + ":文件是否包涵判断" -> (path + "文件夹中不存在" + fileName + "文件。"))
    flg
  }
  /**
   * 删除HDFS文件
   */
  def isDeletHdfsFile(deletpath: String) = {
    val path = new Path(deletpath)
    if (isFileExist(path)) {
      val flg = HDFSFileSytem.delete(path, true)
      if (!flg) mesgList += (dateFormat.format(System.currentTimeMillis) + ":删除HDFS文件" -> (deletpath + " 删除失败。"))
      flg
    } else true
  }

  /**
   * 删除HDFS文件
   */
  def deletHdfsFiles(pathList: Array[String]) = {
    val hdfsPathList = pathList.map(f => new Path(f))
    val shibaiList = hdfsPathList.map(deletpath => (HDFSFileSytem.delete(deletpath, true), deletpath)).filter(!_._1).map(_._2.toString)
    if (shibaiList.size > 0) {
      mesgList += (dateFormat.format(System.currentTimeMillis) + ":删除HDFS文件" -> (shibaiList.mkString(",") + " 删除失败。"))
      false
    } else true
  }

  /**
   * 向HDFS文件写入content
   */
  def writeToHdfsFile(file: String, content: String) = {
    try {
      val out = HDFSFileSytem.create(new Path(file))
      out.write(content.getBytes("UTF-8"))
      out.close
    } catch {
      case e: Exception => {
        mesgList += (dateFormat.format(System.currentTimeMillis) + ":向HDFS写文件" -> (file + " 写入失败。"))
        e.printStackTrace()
      }
    }
  }

  /**
   * 向HDFS文件写入content
   */
  def writeToHdfsFiles(fileList: Array[(String, String)]) = {
    val falgList = fileList.map(info => {
      try {
        val out = HDFSFileSytem.create(new Path(info._1))
        out.write(info._2.getBytes("UTF-8"))
        out.close
        true
      } catch {
        case e: Exception =>
          {
            mesgList += (dateFormat.format(System.currentTimeMillis) + ":向HDFS写文件" -> (info._1 + " 写入失败。"))
            e.printStackTrace()
          }
          false
      }
    })
    if (falgList.contains(false)) false else true
  }

  /**
   * 本目录（文件夹）的最后修改时间
   * date:格式：yyyy-MM-dd
   */
  def isEqualsModifyDate(date: String, file: String) = date == (new SimpleDateFormat("yyyy-MM-dd")).format(HDFSFileSytem.getFileStatus(new Path(file)).getModificationTime)
  /**
   * 文件是否存在
   */
  def isFileExist(path: Path) = HDFSFileSytem.exists(path)
}