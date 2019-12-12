// Case classes for wizard configurations.
package com.lynxanalytics.biggraph.controllers

import play.api.libs.json

case class WizardStep(
    title: String,
    description: String,
    box: String,
    popup: String)

object WizardSteps {
  implicit val fWizardStep = json.Json.format[WizardStep]
}
