package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.serving

import securesocial.{ core => ss }

case class DemoModeStatusResponse(
  demoMode: Boolean)

class DemoModeController(environment: BigGraphEnvironment) {
  def demoModeStatus(user: ss.Identity, req: serving.Empty): DemoModeStatusResponse = {
    DemoModeStatusResponse(!environment.dataManager.computationAllowed)
  }

  def enterDemoMode(user: ss.Identity, req: serving.Empty): Unit = {
    environment.dataManager.computationAllowed = false
  }

  def exitDemoMode(user: ss.Identity, req: serving.Empty): Unit = {
    environment.dataManager.computationAllowed = true
  }
}
