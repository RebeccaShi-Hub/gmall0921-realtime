package com.atguigu.gmall0921.realtime.utils

import java.io.{FileNotFoundException, IOException, InputStream}
import java.util.Properties

import org.junit.Assert.{assertEquals, assertTrue, fail}
import org.junit.Test

/**
 * PropertiesUtil.load 的自动化测试。
 *
 * 覆盖：
 *  1. UTF-8 配置的正常读取；
 *  2. 资源不存在时抛出包含资源名的清晰异常；
 *  3. 正常读取与读取异常两种路径下都关闭底层流。
 *
 * 测试只使用 classpath 资源与内存流，不连接 Kafka、Redis 或 Elasticsearch。
 */
class PropertiesUtilTest {

  private val utf8ResourceName = "properties-util-utf8.properties"
  private val missingResourceName = "no-such-config-file.properties"
  private val brokenResourceName = "broken-config.properties"

  /** 读取时始终抛出 IOException 的内存流，用于模拟读取异常。 */
  private class FailingInputStream extends InputStream {
    override def read(): Int = throw new IOException("模拟读取配置失败")

    override def read(b: Array[Byte], off: Int, len: Int): Int =
      throw new IOException("模拟读取配置失败")
  }

  /**
   * 用包装类加载器替换当前线程的上下文类加载器：请求 resourceName 时返回
   * streamFactory() 生成的流并统计 close 次数，其余请求委托原类加载器。
   * body 执行完（含异常）后恢复原类加载器，返回统计到的 close 次数。
   */
  private def withTrackingClassLoader(resourceName: String,
                                      streamFactory: () => InputStream)
                                     (body: => Unit): Int = {
    var closeCount = 0
    val thread = Thread.currentThread()
    val original = thread.getContextClassLoader
    val wrapper = new ClassLoader(original) {
      override def getResourceAsStream(name: String): InputStream =
        if (name == resourceName) {
          val delegate = streamFactory()
          new InputStream {
            override def read(): Int = delegate.read()

            override def read(b: Array[Byte], off: Int, len: Int): Int =
              delegate.read(b, off, len)

            override def available(): Int = delegate.available()

            override def close(): Unit = {
              closeCount += 1
              delegate.close()
            }
          }
        } else {
          super.getResourceAsStream(name)
        }
    }
    thread.setContextClassLoader(wrapper)
    try {
      body
    } finally {
      thread.setContextClassLoader(original)
    }
    closeCount
  }

  @Test
  def loadReadsUtf8PropertiesFromRealClasspath(): Unit = {
    val properties: Properties = PropertiesUtil.load(utf8ResourceName)

    assertEquals("张三", properties.getProperty("name"))
    assertEquals("北京", properties.getProperty("city"))
    assertEquals("你好，世界", properties.getProperty("greeting"))
    assertEquals("plain-ascii-value", properties.getProperty("ascii.key"))
  }

  @Test
  def loadThrowsFileNotFoundExceptionWithResourceNameWhenMissing(): Unit = {
    try {
      PropertiesUtil.load(missingResourceName)
      fail(s"资源 $missingResourceName 不存在，应抛出 FileNotFoundException")
    } catch {
      case e: FileNotFoundException =>
        assertTrue(s"异常消息应包含资源名，实际是: ${e.getMessage}",
          e.getMessage != null && e.getMessage.contains(missingResourceName))
    }
  }

  @Test
  def loadClosesStreamAfterSuccessfulRead(): Unit = {
    val realLoader = Thread.currentThread().getContextClassLoader
    val closeCount = withTrackingClassLoader(
      utf8ResourceName, () => realLoader.getResourceAsStream(utf8ResourceName)) {
      val properties: Properties = PropertiesUtil.load(utf8ResourceName)
      assertEquals("张三", properties.getProperty("name"))
    }

    assertTrue(s"正常读取完成后应关闭底层流 1 次，实际关闭 $closeCount 次",
      closeCount == 1)
  }

  @Test
  def loadClosesStreamWhenReadFails(): Unit = {
    val closeCount = withTrackingClassLoader(
      brokenResourceName, () => new FailingInputStream) {
      try {
        PropertiesUtil.load(brokenResourceName)
        fail("读取异常时应向上抛出 IOException，而不是吞掉")
      } catch {
        case e: IOException =>
          assertTrue(s"异常应来自模拟的读取失败，实际消息: ${e.getMessage}",
            e.getMessage != null && e.getMessage.contains("模拟读取配置失败"))
      }
    }

    assertTrue(s"读取异常时也应关闭底层流 1 次，实际关闭 $closeCount 次",
      closeCount == 1)
  }
}
