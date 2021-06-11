// Log the value of accessed environment variables, unless
// their contents are confidential
package com.lynxanalytics.biggraph.graph_util

import com.lynxanalytics.biggraph.{bigGraphLogger => log}

object LoggedEnvironment {
  def envOrElse(name: String, alt: String, confidential: Boolean = false) = synchronized {
    val result = scala.util.Properties.envOrElse(name, alt)
    if (!confidential) {
      log.info(s"Environment variable: $name=$result")
    }
    result
  }

  def envOrNone(name: String, confidential: Boolean = false) = synchronized {
    val result = scala.util.Properties.envOrNone(name)
    if (!confidential) {
      log.info(s"Environment variable: $name=$result")
    }
    result
  }
}
