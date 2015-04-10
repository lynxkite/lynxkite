// Case classes for saving the visualizations.
package com.lynxanalytics.biggraph.controllers

import play.api.libs.json

/**
 * The UIStatus class corresponds to Side.status in the UI code, as in a JSON (de)serialization
 * should convert between the two.
 */
case class UIFilterStatus(
  vertex: Map[String, String],
  edge: Map[String, String])
case class UIAxisOptions(
  vertex: Map[String, String],
  edge: Map[String, String])
case class UIAnimation(
  enabled: Boolean,
  labelAttraction: String)
case class UIAttributeFilter(
  val attributeName: String,
  val valueSpec: String)
case class UICenterRequest(
  count: Int,
  filters: Seq[UIAttributeFilter])
case class UIStatus(
  graphMode: String,
  display: String,
  filters: UIFilterStatus,
  bucketCount: String,
  axisOptions: UIAxisOptions,
  sampleRadius: String,
  attributeTitles: Map[String, String],
  animate: UIAnimation,
  // For explicit center ids entered by the user, this will be set.
  centers: Option[Seq[String]],
  // For centers set by a getCenter request, the following parameters will be set
  // so that we can redo the getCenter request.
  lastCentersRequest: Option[UICenterRequest])
object UIStatusSerialization {
  implicit val rUIFilterStatus = json.Json.reads[UIFilterStatus]
  implicit val rUIAxisOptions = json.Json.reads[UIAxisOptions]
  implicit val rUIAnimation = json.Json.reads[UIAnimation]
  implicit val rUIAttributeFilter = json.Json.reads[UIAttributeFilter]
  implicit val rUICenterRequest = json.Json.reads[UICenterRequest]
  implicit val rUIStatus = json.Json.reads[UIStatus]
  implicit val wUIFilterStatus = json.Json.writes[UIFilterStatus]
  implicit val wUIAxisOptions = json.Json.writes[UIAxisOptions]
  implicit val wUIAnimation = json.Json.writes[UIAnimation]
  implicit val wUIAttributeFilter = json.Json.writes[UIAttributeFilter]
  implicit val wUICenterRequest = json.Json.writes[UICenterRequest]
  implicit val wUIStatus = json.Json.writes[UIStatus]
}

