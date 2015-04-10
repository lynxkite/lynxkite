// The HTTP request handler to switch "demo mode" on/off.
//
// Demo mode disables the use of Spark for computation. The DataManager will return errors
// whenever it would have to execute an operation. The idea with demo mode is that we can
// calculate everything on a cluster as we prepare for a demo, and then copy all the scalars
// to a laptop and use demo mode to present everything from the laptop at the client.

package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph.BigGraphEnvironment
import com.lynxanalytics.biggraph.serving

case class DemoModeStatusResponse(
  demoMode: Boolean)

class DemoModeController(environment: BigGraphEnvironment) {
  def demoModeStatus(user: serving.User, req: serving.Empty): DemoModeStatusResponse = {
    DemoModeStatusResponse(!environment.dataManager.computationAllowed)
  }

  def enterDemoMode(user: serving.User, req: serving.Empty): Unit = {
    environment.dataManager.computationAllowed = false
  }

  def exitDemoMode(user: serving.User, req: serving.Empty): Unit = {
    environment.dataManager.computationAllowed = true
  }
}
