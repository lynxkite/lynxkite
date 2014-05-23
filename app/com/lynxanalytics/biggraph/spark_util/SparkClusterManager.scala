package com.lynxanalytics.biggraph.spark_util

import org.apache.spark.SparkContext
import scala.io.Source
import scala.util.Random
import scala.util.Try
import scala.sys.process._

case class SlaveStatus(machineType: String, seenBySparkMaster: Boolean) {}

case class ManagedSparkCluster(clusterName: String,
                               applicationName: String,
                               masterMachineType: String = "n1-standard-1",
                               slaveMachineType: String = "n1-standard-1") {
  val slaveNamePrefix = clusterName + "-"
  val masterName = clusterName + "-master"
  val clusterURL = "spark://" + masterName + ":7077"
  val clusterUI = "http://" + masterName + ":8080"

  def isClusterUp: Boolean =
    Try(Source.fromURL(clusterUI).mkString).getOrElse("").contains(clusterURL)

  def startUpCluster: Unit = {
    synchronized {
      Seq("gcutil",
          "--service_version=v1",
          "--project=big-graph-gc1",
          "adddisk",
          "--zone=europe-west1-b",
          "--source_snapshot=spark-0-9-1-master",
          masterName).!

      Seq("gcutil",
          "--service_version=v1",
          "--project=big-graph-gc1",
          "addinstance",
          "--zone=europe-west1-b",
          s"--machine_type=$masterMachineType",
          "--network=default",
          "--external_ip_address=ephemeral",
          "--service_account_scopes=https://www.googleapis.com/auth/userinfo.email,https://www.googleapis.com/auth/compute,https://www.googleapis.com/auth/devstorage.full_control",
          s"--disk=$masterName,deviceName=$masterName,mode=READ_WRITE,boot",
          masterName).!
    }
  }

  def shutDownCluster: Unit = {
    synchronized {
      killAllSlaves()
      Seq("gcutil",
          "deleteinstance",
          "-f",
          "--delete_boot_pd",
          masterName).!
    }
  }

  private var internalSparkContext: SparkContext = null
  def sparkContext: SparkContext = internalSparkContext

  def connect: SparkContext = {
    assert(internalSparkContext == null)
    internalSparkContext = BigGraphSparkContext(applicationName, clusterURL)
    internalSparkContext
  }

  def disconnect: Unit = {
    assert(internalSparkContext != null)
    internalSparkContext.stop
    internalSparkContext = null
  }

  def numInstances: Int = {
    synchronized {
      runningSlaveInstances.size
    }
  }

  def setNumInstances(desiredNumInstances: Int): Unit = {
    synchronized {
      val instances = runningSlaveInstances
      startSlaves((0 until (desiredNumInstances - instances.size)).map(i => nextInstanceName))
      killSlaves(Random.shuffle(instances).dropRight(desiredNumInstances))
    }
  }

  private def nextInstanceName: String = {
    Random.alphanumeric.take(5).mkString.toLowerCase
  }

  private def startSlaves(slaveNames: Seq[String]): Unit = {
    if (slaveNames.isEmpty) return

    val fullSlaveNames = slaveNames.map(slaveNamePrefix + _)
    val diskCmd =
      Seq("gcutil",
          "--service_version=v1",
          "--project=big-graph-gc1",
          "adddisk",
          "--zone=europe-west1-b",
          "--source_snapshot=spark-0-9-1-slave") ++ fullSlaveNames
    diskCmd.!
    for (slaveName <- fullSlaveNames) {
    val instanceCmd: Seq[String] =
      Seq("gcutil",
          "--service_version=v1",
          "--project=big-graph-gc1",
          "addinstance",
          "--zone=europe-west1-b",
          s"--machine_type=$slaveMachineType",
          "--network=default",
          "--external_ip_address=ephemeral",
          "--service_account_scopes=https://www.googleapis.com/auth/userinfo.email,https://www.googleapis.com/auth/compute,https://www.googleapis.com/auth/devstorage.full_control",
          s"--disk=$slaveName,deviceName=$slaveName,mode=READ_WRITE,boot",
          "--nowait_until_running",
          slaveName)
      instanceCmd.!
    }
  }

  private def killSlaves(slaveNames: Seq[String]): Unit = {
    if (slaveNames.isEmpty) return
    val fullNames = slaveNames.map(slaveNamePrefix + _)
    (Seq("gcutil",
         "deleteinstance",
         "-f",
         "--delete_boot_pd") ++ fullNames).!
  }

  private def runningSlaveInstances(): Seq[String] = {
    val cmd = Seq("sh", "-c",
      "gcutil listinstances --columns name 2> /dev/null | " +
      "grep -v \"+-\"")
    val slave_list = cmd.!!
      slave_list.split("\n")
      .filter(!_.isEmpty())
      .map { slave_line =>
        val Array(_, name) = slave_line.split("\\|").map(_.trim)
        name
      }
      .filter(_.startsWith(slaveNamePrefix))
      .map(_.drop(slaveNamePrefix.length))
      .filter(_ != "master")
  }

  private def killAllSlaves() {
    killSlaves(runningSlaveInstances())
  }
}
