package org.apache.spark.common.util

import org.I0Itec.zkclient.ZkClient
import kafka.utils.ZKStringSerializer
import org.apache.zookeeper.CreateMode
import org.slf4j.LoggerFactory

class ZookeeperUtil(val zk: String) {
  val PERSISTENT = CreateMode.PERSISTENT //短暂，持久
  val EPHEMERAL = CreateMode.EPHEMERAL
  var zkClient: ZkClient = getzkClient(zk)
  lazy val LOG = LoggerFactory.getLogger("ZookeeperUtil")
  /**
   * @author LMQ
   * @func 获取zookeeper的client
   */
  def getzkClient(zk: String) = {
    if (zkClient == null) {
      zkClient = new ZkClient(zk, 10000, 10000, ZKStringSerializer)
    }
    zkClient
  }
  /**
   * @author LMQ
   * @func 判断是否存在某个目录
   */
  def isExist(path: String) = {
    zkClient.exists(path)
  }
  /**
   * @author LMQ
   * @func 删除某个路径
   */
  def deletePath(path: String) = {
    zkClient.delete(path)
  }
  /**
   * @author LMQ
   * @func 创建目录，如果不存在就创建。
   */
  def createFileOrDir(
    path: String,
    data: String = ""): Either[String, Boolean] = {
    if (!zkClient.exists(path)) {
      try {
        zkClient.create(path, data, PERSISTENT)
        new Right(true)
      } catch {
        case t: Throwable =>
          LOG.error("多级目录请使用 createMultistagePath 方法")
          LOG.error(t.toString())
          new Left(t.toString() + "\n" + t.getStackTraceString)
      }
    } else new Right(true)
  }
  /**
   * @author LMQ
   * @func 创建多级目录
   */
  def createMultistagePath(path: String) {
    val paths = path.split("/")
    var curentpath = ""
    paths.foreach { file =>
      if (!file.isEmpty()) {
        curentpath = curentpath + "/" + file
        createFileOrDir(curentpath)
      }
    }
  }
  /**
   * @author LMQ
   * @func 读取文件
   */
  def readData(
    path: String) = {
    zkClient.readData(path).toString()
  }
  /**
   * @author LMQ
   * @func 写入文件
   */
  def writeData(
    path: String,
    data: String) = {
    if (!zkClient.exists(path)) {
      zkClient.create(path, data, PERSISTENT)
    }
    zkClient.writeData(path, data)
  }
}