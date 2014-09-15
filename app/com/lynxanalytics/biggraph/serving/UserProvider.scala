package com.lynxanalytics.biggraph.serving

import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import securesocial.core._
import securesocial.core.providers.Token

class UserProvider(application: play.api.Application) extends UserServicePlugin(application) {
  private val users = collection.mutable.Map[IdentityId, Identity]()
  private val tokens = collection.mutable.Map[String, Token]()

  def find(id: IdentityId): Option[Identity] = {
    users.get(id)
  }

  def findByEmailAndProvider(email: String, providerId: String): Option[Identity] = {
    users.values.find(u => u.email == Some(email) && u.identityId.providerId == providerId)
  }

  def save(user: Identity): Identity = {
    log.info(s"Login from $user")
    users(user.identityId) = user
    user
  }

  def save(token: Token) = {
    tokens(token.uuid) = token
  }

  def findToken(uuid: String): Option[Token] = {
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
