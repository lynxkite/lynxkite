package com.lynxanalytics.biggraph.serving

import org.apache.commons.io.FileUtils
import play.api.libs.json
import securesocial.{ core => ss }

import com.lynxanalytics.biggraph.{ bigGraphLogger => log }

class UserProvider(application: play.api.Application) extends ss.UserServicePlugin(application) {
  private val users = collection.mutable.Map[ss.IdentityId, ss.Identity]()
  private val tokens = collection.mutable.Map[String, ss.providers.Token]()
  private val usersFile = new java.io.File(System.getProperty("user.dir") + "/conf/users.txt")
  private val pwProvider = ss.providers.UsernamePasswordProvider.UsernamePassword

  // Loads user+pass data from usersFile.
  private def loadUsers() = {
    val hasher = new ss.providers.utils.BCryptPasswordHasher(application)
    val defaultPassword = hasher.hash("the lynx is a big cat")
    val data = FileUtils.readFileToString(usersFile, "utf8")
    val passwords = json.Json.parse(data).as[json.JsObject].fields.map {
      case (name, value) => name -> value.as[String]
    }
    for ((user, pass) <- passwords) {
      // The values in users.txt are hashes. An empty value means no password has been set.
      val pwInfo = if (pass.isEmpty) {
        defaultPassword // Use the default password in this case until the user changes it.
      } else {
        defaultPassword.copy(password = pass)
      }
      val id = ss.IdentityId(user, pwProvider)
      users(id) = ss.SocialUser(
        id, user, user, user, Some(user), None, ss.AuthenticationMethod.UserPassword,
        passwordInfo = Some(pwInfo))
    }
    log.info(s"User data loaded from $usersFile.")
  }

  // Saves user+pass data to usersFile.
  private def saveUsers() = {
    val passwords = users.flatMap {
      case (id, identity) if id.providerId == pwProvider =>
        Some(id.userId -> identity.passwordInfo.get.password)
    }
    val data = json.Json.prettyPrint(json.JsObject(
      passwords.mapValues(json.JsString(_)).toSeq
    ))
    FileUtils.writeStringToFile(usersFile, data, "utf8")
    log.info(s"User data saved to $usersFile.")
  }

  // Load data on startup.
  loadUsers()

  def find(id: ss.IdentityId): Option[ss.Identity] = {
    users.get(id)
  }

  def findByEmailAndProvider(email: String, providerId: String): Option[ss.Identity] = {
    users.values.find(u => u.email == Some(email) && u.identityId.providerId == providerId)
  }

  def save(user: ss.Identity): ss.Identity = {
    log.info(s"Login from $user")
    users(user.identityId) = user
    if (user.identityId.providerId == pwProvider) {
      // It is probably a password change. Write the usersFile.
      saveUsers()
    }
    user
  }

  def save(token: ss.providers.Token) = {
    tokens(token.uuid) = token
  }

  def findToken(uuid: String): Option[ss.providers.Token] = {
    tokens.get(uuid)
  }

  def deleteToken(uuid: String) {
    tokens -= uuid
  }

  def deleteTokens() = {
    tokens.clear
  }

  def deleteExpiredTokens() = {
    tokens.retain((_, token) => !token.isExpired)
  }
}
