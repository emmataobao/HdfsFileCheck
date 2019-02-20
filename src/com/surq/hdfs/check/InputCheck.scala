package com.surq.hdfs.check

import org.apache.commons.mail.HtmlEmail
import org.apache.commons.mail.EmailException
import org.apache.commons.mail.DefaultAuthenticator

/**
 * 格式dele|find=/sources/file1,/sources/file2,......
 * count=/sources/file1@N,/sources/file2@N
 */
object InputCheck extends App {
  // 所有参数命令
  val Array(delete, count, write, wait_exist, toEmail, wait_count) = Array("delete", "count", "write", "wait_exist", "toEmail", "wait_count")
  if (args.size > 0) {
    val properties = LoadFilePath.loadProperties("system.properties")
    // 线程堵塞休眠时间
    val waitTime = 30
    val sortIndexList = Array(
      (toEmail, 0),
      (delete, properties.getProperty("action.delete.sort.index", "100").toInt),
      (count, properties.getProperty("action.count.sort.index", "100").toInt),
      (write, properties.getProperty("action.write.sort.index", "100").toInt),
      (wait_exist, properties.getProperty("action.wait_exist.sort.index", "100").toInt),
      (wait_count, properties.getProperty("action.wait_count.sort.index", "100").toInt))
    // 升序
    val paramList = sortIndexList.sortBy(_._2).map(_._1)
    // 参数解析
    val actionType = args.map(_.trim).filter(_ != "").mkString("#").split("--").filter(_ != "").map(param => {
      val par_param = param.split("#", 2)
      val key = par_param(0).trim
      val paramValue = par_param(1).trim
      val value = if (paramValue.endsWith("#")) paramValue.substring(0, paramValue.length - 1) else paramValue
      (key, value.split(','))
    }).toMap
    // 初始化HDFS的工具类
    var hdfsFileCheckUtil: HdfsFileCheckUtil = null
    // action执行
    val flg_list = paramList.map(para => {
      val value = actionType.getOrElse(para, Array[String]())
      para match {
        case "toEmail"    => { hdfsFileCheckUtil = new HdfsFileCheckUtil(value); true }
        case "wait_exist" => { hdfsFileCheckUtil.waitFilesExist(value, waitTime); true }
        case "wait_count" => { hdfsFileCheckUtil.waitFilesCount(value.map(line => { val countN = line.split("#"); (countN(0).trim, countN(1).trim.toInt) }), waitTime); true }
        case "count"      => hdfsFileCheckUtil.isFilesCount(value.map(line => { val countN = line.split("#"); (countN(0).trim, countN(1).trim.toInt) }))
        case "delete"     => hdfsFileCheckUtil.isDeletHdfsFiles(value)
        case "write"      => hdfsFileCheckUtil.writeToHdfsFiles(value.map(line => { val text = line.split("#"); (text(0).trim, text(1).trim) }))
        case _            => { Console println para + "为无效参数"; false }
      }
    })
    // 发邮件
    hdfsFileCheckUtil.sendEmail
    // 只有三种非堵塞的处理需要参数返回值
    val returnParamList = Array("delete", "count", "write")
    //所有参数返回值
    val allParamMap = paramList.zip(flg_list).toMap
    //实际需参数返回值列表 打印到控制台
    actionType.map(_._1).filter(key => returnParamList.contains(key)).toList.map(p => p + ":" + allParamMap(p)) foreach println
  } else {
    Console println "参数格式："
    Console println s"--$delete /sources/file1,/sources/file2,......欲删除路径之间用逗号隔开"
    Console println s"--$wait_exist /sources/file1,/sources/file2,......线程堵塞，欲等待文件路径之间用逗号隔开,所检查文件不到位时每30秒查一次"
    Console println s"--$wait_count /sources/file1,/sources/file2,......线程堵塞，判断文件夹下文件个数，N为期望的个数"
    Console println s"--$count /sources/file1 N,/sources/file2 N,......判断文件夹下文件个数，N为期望的个数"
    Console println s"--$write /sources/file1 text,/sources/file2 text,......向HDFS文件中写入内容。"
    Console println s"--$toEmail surongquan@moxiu.net,......报警邮件接收人地址，用逗号隔开，不配置将不发送邮件"
  }
}