package com.atguigu.gmall0921.realtime.utils

import java.io.{FileNotFoundException, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.util.Properties

object PropertiesUtil {
  def main(args: Array[String]): Unit = {
    val properties: Properties =  PropertiesUtil.load("config.properties")
    println(properties.getProperty("kafka.broker.list"))
  }

  def load(propertieName:String): Properties ={
    val prop=new Properties();
    val inputStream = Thread.currentThread().getContextClassLoader.
      getResourceAsStream(propertieName)
    // 资源不存在时尽早抛出包含资源名的清晰异常，避免后续难定位的空指针异常
    if (inputStream == null) {
      throw new FileNotFoundException(s"classpath 中不存在资源: $propertieName")
    }
    val reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8)
    try {
      prop.load(reader)
      prop
    } finally {
      // 关闭 reader 会同时关闭其底层流，正常读取与读取异常时都释放资源
      reader.close()
    }
  }

}
