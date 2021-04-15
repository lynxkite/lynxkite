// Password and OAuth user authentication.
package com.lynxanalytics.biggraph.serving

import com.lynxanalytics.biggraph.graph_util.LoggedEnvironment
import org.apache.commons.io.FileUtils
import play.api.libs.json
import play.api.libs.Crypto
import play.api.mvc
import org.mindrot.jbcrypt.BCrypt

import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.SparkFreeEnvironment
import com.lynxanalytics.biggraph.controllers._

object User {
  val dir = "Users"
  val singleuser = User(
    email = "(single-user)", isAdmin = true, wizardOnly = false, home = "/")
  val notLoggedIn = User(
    email = "(not logged in)",
    isAdmin = false,
    auth = "none",
    home = util.Properties.envOrElse("KITE_HOME_WITHOUT_LOGIN", dir + "/(not logged in)"),
    wizardOnly = util.Properties.envOrElse("KITE_WIZARD_ONLY_WITHOUT_LOGIN", "") == "yes")
  def apply(
    email: String, isAdmin: Boolean, wizardOnly: Boolean, auth: String = "none",
    home: String = null): User =
    new User(email, isAdmin, wizardOnly, auth, if (home == null) s"$dir/$email" else home) {}
}
// Abstract so that it doesn't generate an apply() method.
abstract case class User(email: String, isAdmin: Boolean, wizardOnly: Boolean, auth: String, home: String) {
  override def toString = email
}

case class UserList(users: List[User])
case class UserOnDisk(email: String, hash: String, isAdmin: Boolean, wizardOnly: Option[Boolean]) {
  def toUser: User = User(email, isAdmin, wizardOnly.getOrElse(false))
}
case class CreateUserRequest(email: String, password: String, isAdmin: Boolean, wizardOnly: Boolean)
case class ChangeUserPasswordRequest(oldPassword: String, newPassword: String, newPassword2: String)
case class ChangeUserRequest(
    email: String, password: Option[String], isAdmin: Option[Boolean], wizardOnly: Option[Boolean])
case class DeleteUserRequest(email: String)

class SignedToken private (signature: String, timestamp: Long, val token: String) {
  override def toString = s"$signature $timestamp $token"
}
object SignedToken {
  val maxAge = {
    val config = play.api.Play.current.configuration
    config.getInt("authentication.cookie.validDays").getOrElse(1) * 24 * 3600
  }

  def apply(): SignedToken = {
    val token = Crypto.generateToken
    val timestamp = time
    new SignedToken(Crypto.sign(s"$timestamp $token"), timestamp, token)
  }

  private def time = java.lang.System.currentTimeMillis / 1000
  private def split(s: String): Option[(String, String)] = {
    val ss = s.split(" ", 2)
    if (ss.size != 2) None
    else Some((ss(0), ss(1)))
  }

  def unapply(s: String): Option[SignedToken] = {
    split(s).flatMap {
      case (signature, timedToken) =>
        if (signature != Crypto.sign(timedToken)) None
        else split(timedToken).flatMap {
          case (timestamp, token) =>
            val tsOpt = util.Try(timestamp.toLong).toOption
            tsOpt.flatMap {
              case ts if ts + maxAge < time => None // Token has expired.
              case ts => Some(new SignedToken(signature, ts, token))
            }
        }
    }
  }
}

object LDAPProps {
  def url = util.Properties.envOrNone("LDAP_URL")
  def authentication = util.Properties.envOrNone("LDAP_AUTHENTICATION")
  def principalTemplate = util.Properties.envOrNone("LDAP_PRINCIPAL_TEMPLATE")
  def hasLDAP = url.nonEmpty && authentication.nonEmpty && principalTemplate.nonEmpty
}

object GoogleAuth {
  val hostedDomain = util.Properties.envOrElse("KITE_GOOGLE_HOSTED_DOMAIN", "").toLowerCase
  def envToBoolean(variable: String) = util.Properties.envOrElse(variable, "") match {
    case "yes" => true
    case "no" => false
    case "" => false
    case x => throw new AssertionError(
      s"$variable must be 'yes', 'no', or empty (meaning no), but found: $x")
  }
  val wizardOnly = envToBoolean("KITE_GOOGLE_WIZARD_ONLY")
  val requiredSuffix = util.Properties.envOrElse("KITE_GOOGLE_REQUIRED_SUFFIX", "").toLowerCase
  val publicAccess = envToBoolean("KITE_GOOGLE_PUBLIC_ACCESS")
  val clientId = {
    val config = play.api.Play.current.configuration
    config.getString("authentication.google.clientId").getOrElse("")
  }
  if (clientId.nonEmpty) {
    assert(
      publicAccess || requiredSuffix.nonEmpty || hostedDomain.nonEmpty,
      "If you set KITE_GOOGLE_CLIENT_ID, you must also set" +
        " KITE_GOOGLE_REQUIRED_SUFFIX, KITE_GOOGLE_HOSTED_DOMAIN, or KITE_GOOGLE_PUBLIC_ACCESS.")
  }
}

object UserController {
  lazy val trustedPublicKey =
    loadPublicKey(util.Properties.envOrElse("KITE_AUTH_TRUSTED_PUBLIC_KEY_BASE64", ""))
  val accessWithoutLogin = util.Properties.envOrElse("KITE_ACCESS_WITHOUT_LOGIN", "") == "yes"

  def loadPublicKey(publicKeyBase64: String) = {
    val publicKeyBytes = java.util.Base64.getDecoder.decode(publicKeyBase64)
    val keySpec = new java.security.spec.X509EncodedKeySpec(publicKeyBytes)
    java.security.KeyFactory.getInstance("RSA").generatePublic(keySpec)
  }

  def isValidSignature(
    msg: String,
    signatureBase64: String,
    publicKey: java.security.PublicKey = null) = {
    val signatureBytes = java.util.Base64.getDecoder.decode(signatureBase64)
    val sig = java.security.Signature.getInstance("SHA256withRSA")
    sig.initVerify(Option(publicKey).getOrElse(trustedPublicKey))
    sig.update(msg.getBytes)
    sig.verify(signatureBytes)
  }
}

class UserController(val env: SparkFreeEnvironment) extends mvc.Controller {
  implicit val metaManager = env.metaGraphManager
  implicit val fUserOnDisk = json.Json.format[UserOnDisk]

  def get(request: mvc.Request[_]): Option[User] = synchronized {
    val cookie = request.cookies.find(_.name == "auth")
    val user = cookie.map(_.value).collect {
      case SignedToken(signed) => signed
    }.flatMap {
      signed => tokens.get(signed.token)
    }
    if (UserController.accessWithoutLogin) user.orElse(Some(User.notLoggedIn)) else user
  }

  val logout = mvc.Action { request =>
    synchronized {
      val cookie = request.cookies.find(_.name == "auth")
      // Find and forget the signed token(s).
      val signeds = cookie.map(_.value).collect {
        case SignedToken(signed) => signed
      }
      for (signed <- signeds) {
        tokens -= signed.token
      }
      // Clear cookie.
      Redirect("/").withCookies(mvc.Cookie(
        "auth", "", secure = true, maxAge = Some(SignedToken.maxAge)))
    }
  }

  def makeToken(user: User) = {
    createHomeDirectoryIfNotExists(user)
    val signed = SignedToken()
    synchronized {
      tokens(signed.token) = user
    }
    signed
  }

  val passwordLogin = mvc.Action(parse.json) { request =>
    val username = (request.body \ "username").as[String]
    val password = (request.body \ "password").as[String]
    val method = (request.body \ "method").as[String]
    log.info(s"User login attempt by $username.")
    val signed = makeToken(getUser(username, password, method))
    log.info(s"$username logged in successfully.")
    Redirect("/").withCookies(mvc.Cookie(
      "auth", signed.toString, secure = true, maxAge = Some(SignedToken.maxAge)))
  }

  val googleLogin = mvc.Action.async(parse.json) { request =>
    implicit val context = scala.concurrent.ExecutionContext.Implicits.global
    implicit val app = play.api.Play.current
    val idToken = (request.body \ "id_token").as[String]
    val tokeninfo: concurrent.Future[play.api.libs.json.JsValue] = concurrent.Future {
      scalaj.http.Http("https://www.googleapis.com/oauth2/v3/tokeninfo")
        .param("id_token", idToken).asString.body
    }.map(json.Json.parse(_))
    tokeninfo.map { ti =>
      val email = (ti \ "email").as[String]
      log.info(s"User login attempt by $email.")
      val authenticatedDomain = (ti \ "aud").as[String]
      assert(email.endsWith(GoogleAuth.requiredSuffix), s"Permission denied to $email.")
      assert(authenticatedDomain == GoogleAuth.clientId, s"Bad token: $ti")
      if (GoogleAuth.hostedDomain.nonEmpty) {
        val hostedDomain = (ti \ "hd").as[Option[String]]
        assert(hostedDomain == Some(GoogleAuth.hostedDomain), s"Permission denied to $email.")
      }
      // Create signed token for email address.
      val signed = makeToken(
        User(email, isAdmin = false, wizardOnly = GoogleAuth.wizardOnly, auth = "Google"))
      log.info(s"$email logged in successfully.")
      Redirect("/").withCookies(mvc.Cookie(
        "auth", signed.toString, secure = true, maxAge = Some(SignedToken.maxAge)))
    }
  }

  val signedUsernameLogin = mvc.Action { request: mvc.Request[mvc.AnyContent] =>
    val fromQuery = request.queryString.get("token").flatMap(_.headOption)
    val fromBody = request.body.asJson.map(j => (j \ "token").as[String])
    val token = fromQuery.orElse(fromBody).get
    val pattern = raw"(.*?)\|(.*)".r
    token match {
      case pattern(username, signatureBase64) =>
        assert(
          UserController.isValidSignature(username, signatureBase64),
          s"Invalid signature for $username: $signatureBase64")
        val signed = makeToken(User(username, isAdmin = false, wizardOnly = false, auth = "signed"))
        log.info(s"$username logged in with signed username token.")
        Redirect(".").withCookies(mvc.Cookie(
          "auth", signed.toString, secure = true, maxAge = Some(SignedToken.maxAge)))
    }
  }

  private def getLDAPUser(username: String, password: String): User = {
    assert(LDAPProps.hasLDAP, "LDAP authentication is not set up.")
    // LDAP based on http://docs.oracle.com/javase/jndi/tutorial/ldap/security/ldap.html
    import javax.naming.Context
    val env = new java.util.Hashtable[String, String]()
    env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory")
    env.put(Context.PROVIDER_URL, LDAPProps.url.get)
    env.put(Context.SECURITY_AUTHENTICATION, LDAPProps.authentication.get)
    // Create the fully qualified LDAP DN from the template and the username.
    // https://docs.oracle.com/cd/E19182-01/820-6573/ghusi/index.html
    env.put(Context.SECURITY_PRINCIPAL, LDAPProps.principalTemplate.get.replace("<USERNAME>", username))
    env.put(Context.SECURITY_CREDENTIALS, password)
    try {
      // Throws AuthenticationException if the authentication fails.
      new javax.naming.directory.InitialDirContext(env)
      User(username, isAdmin = false, wizardOnly = false, auth = "LDAP")
    } catch {
      case e: javax.naming.AuthenticationException => {
        log.error("Error occurred during LDAP authentication.", e)
        throw new AssertionError("Invalid username or password.")
      }
    }
  }

  private def getLynxKiteUser(username: String, password: String): User = {
    assert(users.contains(username), "Invalid username or password.")
    val user = users(username)
    assert(BCrypt.checkpw(password, user.hash), "Invalid username or password.")
    User(user.email, user.isAdmin, user.wizardOnly.getOrElse(false), auth = "password")
  }

  private def getUser(username: String, password: String, method: String): User = synchronized {
    method match {
      case "ldap" => getLDAPUser(username, password)
      case "lynxkite" => getLynxKiteUser(username, password)
    }
  }

  private def hash(pwd: String): String = {
    BCrypt.hashpw(pwd, BCrypt.gensalt(10))
  }

  private def config(setting: String) = {
    val config = play.api.Play.current.configuration
    config.getString(setting).get
  }

  private val usersFileName = LoggedEnvironment.envOrElse("KITE_USERS_FILE", "")
  assert(usersFileName.nonEmpty, "Please specify KITE_USERS_FILE.")
  private val usersFile = new java.io.File(usersFileName)
  // Access to these mutable collections must be synchronized.
  private val tokens = collection.mutable.Map[String, User]()
  private val users = collection.mutable.Map[String, UserOnDisk]()

  // Loads user data from usersFile.
  private def loadUsers() = synchronized {
    users.clear()
    if (usersFile.exists) {
      val data = FileUtils.readFileToString(usersFile, "utf8")
      users ++= json.Json.parse(data).as[Seq[UserOnDisk]].map {
        u => u.email -> u
      }
      log.info(s"User data loaded from $usersFile.")
    } else {
      log.info(s"$usersFile does not exist. Starting with empty user registry.")
    }
    // Initialize the root directory with default ACLs if not exists
    createDirIfNotExists("", "*", "")
    // Create home directories for users who don't have it
    for (u <- users.values) {
      createHomeDirectoryIfNotExists(u.toUser)
    }
  }

  private def createDirIfNotExists(dirName: String, readACL: String, writeACL: String) = {
    val entry = DirectoryEntry.fromName(dirName)
    if (!entry.exists) {
      val dir = entry.asNewDirectory()
      dir.readACL = readACL
      dir.writeACL = writeACL
    }
  }

  private def createHomeDirectoryIfNotExists(u: User) = {
    createDirIfNotExists(User.dir, "*", "")
    createDirIfNotExists(u.home, u.email, u.email)
  }

  // Saves user data to usersFile.
  private def saveUsers() = synchronized {
    // Keep users sorted on the disk and therefore also on the UI.
    val data = json.Json.prettyPrint(json.Json.toJson(users.toSeq.sortBy(_._1).map(_._2)))
    FileUtils.writeStringToFile(usersFile, data, "utf8")
    log.info(s"User data saved to $usersFile.")
  }

  // Change password
  def changeUserPassword(user: User, req: ChangeUserPasswordRequest): Unit = synchronized {
    assert(BCrypt.checkpw(req.oldPassword, users(user.email).hash), "Incorrect old password.")
    assert(req.newPassword == req.newPassword2, "The two new passwords do not match.")
    assert(req.newPassword.nonEmpty, "The new password cannot be empty.")
    assert(req.newPassword != req.oldPassword, "The new password cannot be the same as the old one.")
    users(user.email) = UserOnDisk(user.email, hash(req.newPassword), user.isAdmin, Some(user.wizardOnly))
    saveUsers()
    log.info(s"Successfully changed password for user ${user.email}.")
  }

  // Change user
  def changeUser(user: User, req: ChangeUserRequest): Unit = synchronized {
    assert(user.isAdmin, "Only admin users can change users.")
    assert(users.contains(req.email), s"User not found '${req.email}'.")
    assert(req.password.isEmpty || req.password.get.nonEmpty, s"The password cannot be empty.")
    val old = users(req.email)
    users(req.email) = UserOnDisk(
      req.email,
      req.password.map { p => hash(p) }.getOrElse(old.hash),
      req.isAdmin.getOrElse(old.isAdmin),
      req.wizardOnly.orElse(old.wizardOnly))
    saveUsers()
    log.info(s"Successfully changed user ${user.email}.")
  }

  def deleteUser(user: User, req: ChangeUserRequest): Unit = synchronized {
    assert(user.isAdmin, "Only admin users can delete users.")
    assert(users.contains(req.email), s"User not found '${req.email}'.")
    assert(user.email != req.email, s"Cannot delete yourself.")
    users -= req.email
    saveUsers()
    log.info(s"Successfully deleted user ${user.email}.")
  }

  // List user names.
  def getUsers(user: User, req: Empty): UserList = synchronized {
    UserList(users.values.toList.sortBy(_.email).map(u => u.toUser))
  }

  // Get the data of the logged in user.
  def getUserData(user: User, req: Empty): User = user

  // Add new user.
  def createUser(user: User, req: CreateUserRequest): Unit = synchronized {
    assert(
      user.isAdmin,
      s"Only administrators can create new users. $user is not an administrator.")
    assert(req.email.nonEmpty, "User name missing")
    assert(req.password.nonEmpty, "Password missing")
    val numbers = "0123456789"
    val letters = "abcdefghijklmnopqrstuvwxyz"
    val allowed = numbers + letters + letters.toUpperCase + "._-@"
    assert(req.email.forall(allowed.contains(_)), "User name contains disallowed characters.")
    assert(!users.contains(req.email), s"User name ${req.email} is already taken.")
    users(req.email) = UserOnDisk(req.email, hash(req.password), req.isAdmin, Some(req.wizardOnly))
    createHomeDirectoryIfNotExists(users(req.email).toUser)
    saveUsers()
    log.info(s"Successfully created user ${req.email}.")
  }

  // Load data on startup.
  loadUsers()
}

trait AccessControl {
  def readAllowedFrom(user: User): Boolean
  def writeAllowedFrom(user: User): Boolean

  def assertReadAllowedFrom(user: User): Unit = {
    if (!readAllowedFrom(user)) {
      if (user == User.notLoggedIn) {
        throw ResultException(mvc.Results.Unauthorized) // Force login.
      } else {
        throw new AssertionError(s"User $user does not have read access to entry '$this'.")
      }
    }
  }
  def assertWriteAllowedFrom(user: User): Unit = {
    if (!writeAllowedFrom(user)) {
      if (user == User.notLoggedIn) {
        throw ResultException(mvc.Results.Unauthorized) // Force login.
      } else {
        throw new AssertionError(s"User $user does not have write access to entry '$this'.")
      }
    }
  }
  def aclContains(acl: String, user: User): Boolean = {
    // The ACL is a comma-separated list of email addresses with '*' used as a wildcard.
    // We translate this to a regex for checking.
    val regex = "\\Q" + acl.trim.replaceAll(" *, *", "\\\\E|\\\\Q").replace("*", "\\E.*\\Q") + "\\E"
    user.email.matches(regex)
  }
}
