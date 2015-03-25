package com.lynxanalytics.biggraph.serving

import org.apache.commons.io.FileUtils
import play.api.libs.json
import play.api.libs.Crypto
import play.api.libs.ws.WS
import play.api.mvc
import org.mindrot.jbcrypt.BCrypt

import com.lynxanalytics.biggraph.{ bigGraphLogger => log }

object User {
  val fake = User("fake", isAdmin = true)
}
case class User(email: String, isAdmin: Boolean) {
  override def toString = email
}
case class UserList(users: List[User])
case class UserOnDisk(email: String, hash: String, isAdmin: Boolean)
case class CreateUserRequest(email: String, password: String, isAdmin: Boolean)

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

object UserProvider extends mvc.Controller {
  implicit val fUserOnDisk = json.Json.format[UserOnDisk]

  def get(request: mvc.Request[_]): Option[User] = synchronized {
    val cookie = request.cookies.find(_.name == "auth")
    cookie.map(_.value).collect {
      case SignedToken(signed) => signed
    }.flatMap {
      signed => tokens.get(signed.token)
    }
  }

  val logout = mvc.Action { request =>
    synchronized {
      val cookie = request.cookies.find(_.name == "auth")
      cookie.map(_.value).collect {
        case SignedToken(signed) => signed
      }.foreach { signed =>
        // Forget token.
        tokens -= signed.token
      }
      // Clear cookie.
      Redirect("/").withCookies(mvc.Cookie(
        "auth", "", secure = true, maxAge = Some(SignedToken.maxAge)))
    }
  }

  val passwordLogin = mvc.Action(parse.json) { request =>
    val username = (request.body \ "username").as[String]
    val password = (request.body \ "password").as[String]
    val signed = SignedToken()
    synchronized {
      val user = getUser(username, password)
      tokens(signed.token) = user
    }
    Redirect("/").withCookies(mvc.Cookie(
      "auth", signed.toString, secure = true, maxAge = Some(SignedToken.maxAge)))
  }

  val googleLogin = mvc.Action.async(parse.json) { request =>
    implicit val context = scala.concurrent.ExecutionContext.Implicits.global
    implicit val app = play.api.Play.current
    val code = (request.body \ "code").as[String]
    // Get access token for single-use code.
    val token: concurrent.Future[String] =
      WS.url("https://accounts.google.com/o/oauth2/token").post(Map(
        "client_id" -> Seq(config("authentication.google.clientId")),
        "client_secret" -> Seq(config("authentication.google.clientSecret")),
        "grant_type" -> Seq("authorization_code"),
        "code" -> Seq(code),
        "redirect_uri" -> Seq("postmessage")))
        .map { response =>
          (response.json \ "access_token").as[String]
        }
    // Use access token to get email address.
    val email = token.flatMap { token =>
      WS.url("https://www.googleapis.com/plus/v1/people/me")
        .withQueryString("fields" -> "id,name,displayName,image,emails", "access_token" -> token)
        .get().map { response =>
          ((response.json \ "emails")(0) \ "value").as[String]
        }
    }
    // Create signed token for email address.
    email.map { email =>
      val signed = SignedToken()
      assert(email.endsWith("@lynxanalytics.com"), s"Permission denied to $email.")
      synchronized {
        tokens(signed.token) = User(email, isAdmin = false)
      }
      Redirect("/").withCookies(mvc.Cookie(
        "auth", signed.toString, secure = true, maxAge = Some(SignedToken.maxAge)))
    }
  }

  private def getUser(username: String, password: String): User = synchronized {
    assert(users.contains(username), "Invalid username or password.")
    val user = users(username)
    assert(BCrypt.checkpw(password, user.hash), "Invalid username or password.")
    User(user.email, user.isAdmin)
  }

  private def hash(pwd: String): String = {
    BCrypt.hashpw(pwd, BCrypt.gensalt(10))
  }

  private def config(setting: String) = {
    val config = play.api.Play.current.configuration
    config.getString(setting).get
  }

  private val usersFile = new java.io.File(System.getProperty("user.dir") + "/conf/users.txt")
  // Access to these mutable collections must be synchronized.
  private val tokens = collection.mutable.Map[String, User]()
  private val users = collection.mutable.Map[String, UserOnDisk]()

  // Loads user data from usersFile.
  private def loadUsers() = synchronized {
    val data = FileUtils.readFileToString(usersFile, "utf8")
    users.clear()
    users ++= json.Json.parse(data).as[Seq[UserOnDisk]].map {
      u => u.email -> u
    }
    log.info(s"User data loaded from $usersFile.")
  }

  // Saves user data to usersFile.
  private def saveUsers() = synchronized {
    val data = json.Json.prettyPrint(json.Json.toJson(users.values))
    FileUtils.writeStringToFile(usersFile, data, "utf8")
    log.info(s"User data saved to $usersFile.")
  }

  // List user names.
  def getUsers(user: User, req: Empty): UserList = synchronized {
    UserList(users.values.toList.sortBy(_.email).map(u => User(u.email, u.isAdmin)))
  }

  // Add new user.
  def createUser(user: User, req: CreateUserRequest): Unit = synchronized {
    assert(user.isAdmin,
      s"Only administrators can create new users. $user is not an administrator.")
    assert(req.email.nonEmpty, "User name missing")
    assert(req.password.nonEmpty, "Password missing")
    val letters = "abcdefghijklmnopqrstuvwxyz"
    val allowed = letters + letters.toUpperCase + "._-@"
    assert(req.email.forall(allowed.contains(_)), "User name contains disallowed characters.")
    assert(!users.contains(req.email), s"User name ${req.email} is already taken.")
    users(req.email) = UserOnDisk(req.email, hash(req.password), req.isAdmin)
    saveUsers()
  }

  // Load data on startup.
  loadUsers()
}
