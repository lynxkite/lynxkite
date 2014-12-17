package com.lynxanalytics.biggraph.serving

import org.apache.commons.io.FileUtils
import play.api.libs.json
import org.mindrot.jbcrypt.BCrypt

import com.lynxanalytics.biggraph.{ bigGraphLogger => log }

class UserProvider(application: play.api.Application) {
  private val usersFile = new java.io.File(System.getProperty("user.dir") + "/conf/users.txt")

  // Loads user+pass data from usersFile.
  private def loadUsers() = {
    val defaultPassword = BCrypt.hashpw("the lynx is a big cat", BCrypt.gensalt(10))
    val data = FileUtils.readFileToString(usersFile, "utf8")
    val passwords = json.Json.parse(data).as[json.JsObject].fields.map {
      case (name, value) => name -> value.as[String]
    }
    for ((user, pass) <- passwords) {
      // The values in users.txt are hashes. An empty value means no password has been set.
      val pwInfo =
        if (pass.nonEmpty) pass
        // Use the default password when one is not defined until the user changes it.
        else defaultPassword
    }
    log.info(s"User data loaded from $usersFile.")
  }

  // Saves user+pass data to usersFile.
  private def saveUsers() = {
    val passwords = Map[String, String]()
    val data = json.Json.prettyPrint(json.JsObject(
      passwords.mapValues(json.JsString(_)).toSeq
    ))
    FileUtils.writeStringToFile(usersFile, data, "utf8")
    log.info(s"User data saved to $usersFile.")
  }

  // Load data on startup.
  loadUsers()
}
