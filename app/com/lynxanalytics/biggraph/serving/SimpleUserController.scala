// Placeholder for an authentication system.
package com.lynxanalytics.biggraph.serving

object User {
  val singleuser = User("(single-user)", isAdmin = true, wizardOnly = false)
  val dir = "Users"
}
case class User(email: String, isAdmin: Boolean, wizardOnly: Boolean, auth: String = "none") {
  override def toString = email
  def home = User.dir + "/" + email
}

trait AccessControl {
  def readAllowedFrom(user: User): Boolean
  def writeAllowedFrom(user: User): Boolean

  def assertReadAllowedFrom(user: User): Unit = {
    assert(readAllowedFrom(user), s"User $user does not have read access to entry '$this'.")
  }
  def assertWriteAllowedFrom(user: User): Unit = {
    assert(writeAllowedFrom(user), s"User $user does not have read access to entry '$this'.")
  }
  def aclContains(acl: String, user: User): Boolean = true
}
