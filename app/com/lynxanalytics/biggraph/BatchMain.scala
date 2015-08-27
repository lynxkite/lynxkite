// BatchMain allows for running lynxkite in batch mode: without web interface, just having it
// execute a workflow.

package com.lynxanalytics.biggraph

import scala.collection.JavaConversions

import com.lynxanalytics.biggraph.frontend_operations.Operations

object BatchMain extends App {
  if (args.size < 1) {
    System.err.println("""
Usage:
./run-kite.sh batch name_of_script_file [parameter_values]

parameter_values is list of items in the format parameter_name:parameter_value

For example:
./run-kite.sh batch my_script.groovy seed:42 input_file_name:data1.csv
""")
    System.exit(-1)
  }
  val scriptFileName :: paramSpecs = args.toList
  val params = paramSpecs
    .map { paramSpec =>
      val colonIdx = paramSpec.indexOf(':')
      assert(
        colonIdx > 0,
        s"Invalid parameter value spec: $paramSpec. " +
          "Parameter values should be specified as name:value")
      (paramSpec.take(colonIdx), paramSpec.drop(colonIdx + 1))
    }
    .toMap

  val env = BigGraphProductionEnvironment
  val ops = new Operations(env)
  val user = serving.User("Batch User", isAdmin = true)
  val commandLine = s"run-kite.sh batch ${args.mkString(" ")}"
  val ctx = groovy.GroovyContext(user, ops, Some(env), Some(commandLine))
  val shell = ctx.trustedShell("params" -> JavaConversions.mapAsJavaMap(params))
  shell.evaluate(new java.io.File(scriptFileName))
}
