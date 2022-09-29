// All configuration settings should be accessed through this class.
// It provides logging and overrides.
package com.lynxanalytics.lynxkite.graph_util

import com.lynxanalytics.lynxkite.{logger => log}

object Environment {
  private var overrides = Map[String, String]()
  def set(settings: (String, String)*): Unit = {
    overrides = overrides ++ settings
  }
  def clear: Unit = {
    overrides = Map()
  }
  def envOrElse(name: String, alt: String, confidential: Boolean = false): String = synchronized {
    val result = overrides.getOrElse(name, scala.util.Properties.envOrElse(name, alt))
    if (!confidential) {
      log.info(s"Environment variable: $name=$result")
    }
    result
  }

  def envOrNone(name: String, confidential: Boolean = false): Option[String] = synchronized {
    val result = overrides.get(name).orElse(scala.util.Properties.envOrNone(name))
    if (!confidential) {
      log.info(s"Environment variable: $name=$result")
    }
    result
  }

  def envOrError(name: String, msg: String, confidential: Boolean = false): String = synchronized {
    envOrNone(name, confidential) match {
      case Some(value) => value
      case None => throw new AssertionError(s"$name $msg")
    }
  }
}
