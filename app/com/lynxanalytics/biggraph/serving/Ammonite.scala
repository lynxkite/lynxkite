// The Ammonite shell you can access via SSH.
package com.lynxanalytics.biggraph.serving

import com.lynxanalytics.biggraph.graph_util.LoggedEnvironment
import org.apache.spark
import com.lynxanalytics.biggraph.BigGraphProductionEnvironment
import com.lynxanalytics.biggraph.{ bigGraphLogger => log }
import com.lynxanalytics.biggraph.controllers._
import org.apache.sshd.server.auth.password.PasswordAuthenticator
import org.apache.sshd.server.session.ServerSession

object Ammonite {
  val env = BigGraphProductionEnvironment

  // Starting Ammonite if requested.
  val help = org.apache.commons.lang.StringEscapeUtils.escapeJava(
    """
============================================================
Welcome to the bellies of LynxKite! Please don't hurt her...
============================================================

This is an Ammonite Scala REPL running in the JVM of the LynxKite server. For generic help
on Ammonite, look here:
https://lihaoyi.github.io/Ammonite/

For convenience, we've set up some Kite specific bindings for you:
 sql: The SqlContext used by Kite. Use it if you want to run some SparkSQL computations
   using Kite's resources. See http://spark.apache.org/docs/latest/sql-programming-guide.html
   for SparkSQL documentation.
 sc: The SparkContext used by Kite. Use it if you want to run some arbitrary spark computation
   using Kite's resources. See http://spark.apache.org/docs/latest/programming-guide.html
   for a good Spark intro.
 server: A reference to the ProductionJsonServer used to serve http requests.
 fakeAdmin: A fake admin user object. Useful if you want to directly interact with controllers.
 dataManager: The DataManager instance used by Kite.
 metaManager: The MetaManager instance used by Kite.
 batch.runScript("name_of_script_file", "param1" -> "value1", "param2" -> "value2", ...): A method
   for running a batch script on the running Kite instance.

Remember, any of the above can be used to easily destroy the running server or even any data.
Drive responsibly.""")

  private val replServer = LoggedEnvironment.envOrNone("KITE_AMMONITE_PORT").map { ammonitePort =>
    import ammonite.util.Bind
    new ammonite.sshd.SshdRepl(
      ammonite.sshd.SshServerConfig(
        // We only listen on the local interface.
        address = "localhost",
        port = ammonitePort.toInt,
        passwordAuthenticator = Some(new AmmoniteAuthenticator(
          LoggedEnvironment.envOrElse("KITE_AMMONITE_USER", "lynx"),
          LoggedEnvironment.envOrElse("KITE_AMMONITE_PASSWD", "kite", confidential = true)))),

      predef = s"""
repl.frontEnd() = ammonite.repl.FrontEnd.JLineUnix
import com.lynxanalytics.biggraph._
println("${help}")
""",
      replArgs = Seq(
        Bind("server", this),
        Bind("fakeAdmin", User("ammonite-ssh", isAdmin = true, wizardOnly = false)),
        Bind("sc", env.sparkContext),
        Bind("metaManager", env.metaGraphManager),
        Bind("dataManager", env.dataManager),
        Bind("sql", env.sparkDomain.masterSQLContext)))
  }

  def maybeStart() = {
    replServer.foreach { s =>
      s.start()
      log.info(s"Ammonite sshd started on port ${s.port}.")
    }
  }

  def maybeStop() = {
    replServer.foreach { s =>
      s.stop()
      log.info("Ammonite sshd stopped.")
    }
  }
}

class AmmoniteAuthenticator(username: String, password: String) extends PasswordAuthenticator {
  override def authenticate(username: String, password: String, session: ServerSession): Boolean = {
    this.username == username && this.password == password
  }
}
