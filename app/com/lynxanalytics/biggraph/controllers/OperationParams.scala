// OperationParameterMeta subclasses that can be used to represent different types of operation
// parameters.
package com.lynxanalytics.biggraph.controllers

import com.lynxanalytics.biggraph._
import com.lynxanalytics.biggraph.serving.FrontendJson
import play.api.libs.json

object OperationParams {
  case class Param(
      id: String,
      title: String,
      defaultValue: String = "",
      override val group: String = "",
      override val placeholder: String = "") extends OperationParameterMeta {
    val kind = "default"
    val options = List()
    val multipleChoice = false
    def validate(value: String): Unit = {}
  }

  case class Choice(
      id: String,
      title: String,
      options: List[FEOption],
      multipleChoice: Boolean = false,
      allowUnknownOption: Boolean = false,
      hiddenOptions: List[FEOption] = Nil, // Not offered on the UI, but not an error.
      override val group: String = "") extends OperationParameterMeta {
    val kind = "choice"
    val defaultValue = if (multipleChoice) "" else options.headOption.map(_.id).getOrElse("")
    def validate(value: String): Unit = {
      if (!allowUnknownOption) {
        val possibleValues = options.map { x => x.id }.toSet
        val hiddenValues = hiddenOptions.map { x => x.id }.toSet
        val givenValues: Set[String] = if (!multipleChoice) Set(value) else {
          if (value.isEmpty) Set() else value.split(",", -1).toSet
        }
        val unknown = givenValues -- possibleValues -- hiddenValues
        assert(
          unknown.isEmpty,
          s"Unknown option for $id: ${unknown.mkString(", ")}" +
            s" (Possibilities: ${possibleValues.mkString(", ")})")
      }
    }
  }

  case class ImportedDataParam() extends OperationParameterMeta {
    val id = "imported_table" // Supports other types now, but we kept the ID for compatibility.
    val title = ""
    val kind = "imported-table"
    val options = List()
    val multipleChoice = false
    val defaultValue = ""
    def validate(value: String): Unit = {}
  }

  case class TriggerBoxParam(
      id: String,
      title: String,
      successMessage: String) extends OperationParameterMeta {
    val kind = "trigger"
    val options = List()
    val multipleChoice = false
    val defaultValue = ""
    def validate(value: String): Unit = {
      assert(value == "")
    }
    override val payload = Some(json.Json.obj(
      "successMessage" -> successMessage))
  }

  case class TagList(
      id: String,
      title: String,
      options: List[FEOption]) extends OperationParameterMeta {
    val kind = "tag-list"
    val multipleChoice = true
    val defaultValue = ""
    def validate(value: String): Unit = {}
  }

  case class FileParam(id: String, title: String) extends OperationParameterMeta {
    val kind = "file"
    val multipleChoice = false
    val defaultValue = ""
    val options = List()
    def validate(value: String): Unit = {}
  }

  case class Ratio(id: String, title: String, defaultValue: String = "")
    extends OperationParameterMeta {
    val kind = "default"
    val options = List()
    val multipleChoice = false
    def validate(value: String): Unit = {
      assert(
        (value matches """\d+(\.\d+)?""") && (value.toDouble <= 1.0),
        s"$title ($value) has to be a ratio, a number between 0.0 and 1.0")
    }
  }

  case class NonNegInt(id: String, title: String, default: Int)
    extends OperationParameterMeta {
    val kind = "default"
    val defaultValue = default.toString
    val options = List()
    val multipleChoice = false
    def validate(value: String): Unit = {
      assert(value matches """\d+""", s"$title ($value) has to be a non negative integer")
    }
  }

  case class NonNegDouble(id: String, title: String, defaultValue: String = "")
    extends OperationParameterMeta {
    val kind = "default"
    val options = List()
    val multipleChoice = false
    def validate(value: String): Unit = {
      assert(value matches """\d+(\.\d+)?""", s"$title ($value) has to be a non negative number")
    }
  }

  case class Code(
      id: String,
      title: String,
      language: String,
      defaultValue: String = "",
      enableTableBrowser: Boolean = false,
      override val group: String = "") extends OperationParameterMeta {
    val kind = "code"
    val options = List()
    val multipleChoice = false
    override val payload = Some(
      json.Json.obj(
        "language" -> language,
        "enableTableBrowser" -> enableTableBrowser))
    def validate(value: String): Unit = {}
  }

  // A random number to be used as default value for random seed parameters.
  // The default seed is picked based on properties of the box.
  case class RandomSeed(id: String, title: String, box: Box) extends OperationParameterMeta {
    val defaultValue = box.id.hashCode.toString
    val kind = "default"
    val options = List()
    val multipleChoice = false
    def validate(value: String): Unit = {
      assert(value matches """[+-]?\d+""", s"$title ($value) has to be an integer")
    }
  }

  case class ModelParams(
      id: String,
      title: String,
      models: Map[String, model.ModelMeta],
      attrs: List[FEOption],
      attrTypes: List[String]) extends OperationParameterMeta {
    val feModels = models.toList.sortBy(_._1).map { case (k, v) => model.Model.toMetaFE(k, v) }
    val defaultValue = {
      val m = feModels.head
      json.Json.obj(
        "modelName" -> m.name,
        "features" -> m.featureNames).toString
    }
    val kind = "model"
    val multipleChoice = false
    val options = List()
    import FrontendJson.wFEModelMeta
    import FrontendJson.fFEOption
    implicit val wModelsPayload = json.Json.writes[ModelsPayload]
    override val payload = Some(json.Json.toJson(ModelsPayload(
      models = feModels,
      attrs = attrs,
      attrTypes = attrTypes)))
    def validate(value: String): Unit = {}
  }

  case class SegmentationParam(
      id: String,
      title: String,
      options: List[FEOption]) extends OperationParameterMeta {
    val kind = "segmentation"
    val multipleChoice = false
    val defaultValue = ""
    def validate(value: String): Unit = {}
  }

  case class ParametersParam(
      id: String,
      title: String) extends OperationParameterMeta {
    val kind = "parameters"
    val defaultValue = ParametersParam.defaultValue
    val multipleChoice = false
    val options = List()
    def validate(value: String): Unit = {
      ParametersParam.parse(Some(value))
    }
  }
  object ParametersParam {
    val defaultValue = "[]"
    def parse(value: Option[String]): Seq[CustomOperationParameterMeta] = {
      import FrontendJson.fCustomOperationParameterMeta
      json.Json.parse(value.getOrElse(defaultValue)).as[List[CustomOperationParameterMeta]]
    }
  }

  case class VisualizationParam(
      id: String,
      title: String,
      defaultValue: String = "") extends OperationParameterMeta {
    val kind = "visualization"
    def validate(value: String): Unit = {
      if (!value.isEmpty) {
        import UIStatusSerialization._
        val j = json.Json.parse(value)
        j.as[TwoSidedUIStatus]
      }
    }
    val multipleChoice = false
    val options = List()
  }

  class DummyParam(
      val id: String,
      changeableTitle: => String) extends OperationParameterMeta {
    def title = changeableTitle
    val kind = "dummy"
    val options = List()
    val multipleChoice = false
    val defaultValue = ""
    def validate(value: String): Unit = {}
  }

  case class WizardStepsParam(
      id: String,
      title: String) extends OperationParameterMeta {
    val kind = "wizard-steps"
    val defaultValue = "[]"
    def validate(value: String): Unit = {
      import WizardSteps._
      val j = json.Json.parse(value)
      j.as[List[WizardStep]]
    }
    val multipleChoice = false
    val options = List()
  }
}

// A special parameter payload to describe applicable models on the UI.
case class ModelsPayload(
    models: List[model.FEModelMeta],
    attrs: List[FEOption],
    attrTypes: List[String])
