package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.serving

case class DemoModeStatusResponse(
  demoMode: Boolean)

class DemoModeController(environment: BigGraphEnvironment) {
  def demoModeStatus(req: serving.Empty): DemoModeStatusResponse = {
    DemoModeStatusResponse(!environment.dataManager.computationAllowed)
  }

  def enterDemoMode(req: serving.Empty): Unit = {
    environment.dataManager.computationAllowed = false
  }

  def exitDemoMode(req: serving.Empty): Unit = {
    environment.dataManager.computationAllowed = true
  }
}
