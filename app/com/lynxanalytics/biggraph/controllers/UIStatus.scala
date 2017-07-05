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
case class UIAttributeAxisOptions(
  logarithmic: Boolean)
case class UIAxisOptions(
  vertex: Map[String, UIAttributeAxisOptions],
  edge: Map[String, UIAttributeAxisOptions])
case class UIAnimation(
  enabled: Boolean,
  style: String,
  labelAttraction: Double)
case class UIAttributeFilter(
  val attributeName: String,
  val valueSpec: String)
case class UICenterRequest(
  count: Int,
  filters: Seq[UIAttributeFilter],
  offset: Option[Int])

case class UIStatus(
  projectPath: Option[String],
  graphMode: Option[String],
  display: String,
  filters: UIFilterStatus,
  bucketCount: Int,
  preciseBucketSizes: Option[Boolean],
  relativeEdgeDensity: Option[Boolean],
  axisOptions: UIAxisOptions,
  sampleRadius: Int,
  attributeTitles: Map[String, String],
  animate: UIAnimation,
  // For explicit center ids entered by the user, this will be set.
  centers: Option[Seq[String]],
  // For centers set by a getCenter request, the following parameters will be set
  // so that we can redo the getCenter request.
  lastCentersRequest: Option[UICenterRequest],
  customVisualizationFilters: Option[Boolean],
  sliderPos: Option[Double])
object UIStatus {
  val default = UIStatus(
    projectPath = None,
    graphMode = None,
    display = "svg",
    filters = UIFilterStatus(vertex = Map(), edge = Map()),
    bucketCount = 4,
    preciseBucketSizes = Some(false),
    relativeEdgeDensity = Some(false),
    axisOptions = UIAxisOptions(vertex = Map(), edge = Map()),
    sampleRadius = 1,
    attributeTitles = Map(), animate = UIAnimation(false, "neutral", 0),
    centers = Some(Seq("auto")),
    lastCentersRequest = None,
    customVisualizationFilters = Some(false),
    sliderPos = None)
}

case class TwoSidedUIStatus(
  left: Option[UIStatus],
  right: Option[UIStatus])
object UIStatusSerialization {
  implicit val fUIFilterStatus = json.Json.format[UIFilterStatus]
  implicit val fUIAttributeAxisOptions = json.Json.format[UIAttributeAxisOptions]
  implicit val fUIAxisOptions = json.Json.format[UIAxisOptions]
  implicit val fUIAnimation = json.Json.format[UIAnimation]
  implicit val fUIAttributeFilter = json.Json.format[UIAttributeFilter]
  implicit val fUICenterRequest = json.Json.format[UICenterRequest]
  implicit val fUIStatus = json.Json.format[UIStatus]
  implicit val fTwoSidedUIStatus = json.Json.format[TwoSidedUIStatus]
}
