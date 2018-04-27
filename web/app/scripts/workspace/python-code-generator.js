// Generates Python API code for selected boxes or for the whole workspace.
'use strict';

angular.module('biggraph').factory('pythonUtil', function() {

  function pythonUtil() {

  }

  pythonUtil.prototype.saveAsPython = function (workspace, selectedBoxIds) {

    function snakeCase(boxId) {
      return boxId.toLowerCase().replace(/-/g, '_');
    }

    function snakeInput(inp) {
      return {
        boxId: snakeCase(inp.boxId),
        id: inp.id};
    }

    function snakeInputs(inputs) {
      var snakeInps = {};
      Object.keys(inputs).forEach(function(key) {
        snakeInps[key] = snakeInput(inputs[key]);
      });
      return snakeInps;
    }

    function pythonName(opId) {
      var cleaned = opId.replace(/[^\w_\s]+/g, '').split(' ');
      const head = cleaned[0];
      const tail = cleaned.slice(1);
      return head.toLowerCase() + tail.map(x => x[0].toUpperCase() + x.slice(1)).join('');
    }

    function removeNonSelectedInputs(boxes) {
      var selected = boxes.map(x => x.id);
      for (var i = 0; i < boxes.length; i++) {
        var box = boxes[i];
        var inputKeys = Object.keys(box.inputs);
        inputKeys.forEach(function(key) {
          if (!selected.includes(box.inputs[key].boxId)) {
            delete box.inputs[key];
          }
        });
      }
    }

    function unconnected(boxes) {
      var input_boxes = [];
      for (var i = 0; i < boxes.length; i++) {
        var box = boxes[i];
        box.inputPlugs.forEach(function(plug) {
          if (!box.inputs[plug]) {
            var input_id = 'input_' + plug + '_for_' + box.id;
            // Now we connect it to a dummy input.
            box.inputs[plug] = {boxId: input_id, id: 'input'};
            input_boxes.push({
              id: input_id,
              op: 'Input',
              pythonOp: 'input',
              parameters: {name: input_id},
              parametricParameters: {},
              inputs: {},
              inputPlugs: [],
              category: 'Workflow'});
          }
        });
      }
      return input_boxes;
    }

    function dependencies(boxes) {
      var deps = {};
      for (var i = 0; i < boxes.length; i++) {
        var box = boxes[i];
        var fromId = box.id;
        deps[fromId] = [];
        Object.keys(box.inputs).forEach(function(key) {
          deps[fromId].push(box.inputs[key].boxId);
        });
      }
      return deps;
    }

    function mapping(boxes) {
      var map = {};
      for (var i = 0; i < boxes.length; i++) {
        map[boxes[i].id] = boxes[i];
      }
      return map;
    }

    function topsort(deps) {
      var sorted = [];
      /* eslint-disable no-constant-condition */
      while (true) {
        var next_group = Object.keys(deps).filter(x => deps[x].length === 0);
        if (next_group.length === 0) {
          break;
        }
        sorted.push.apply(sorted, next_group); // extend
        var updated_deps = {};
        Object.keys(deps).forEach(function(key) {
          if (!next_group.includes(key)) {
            updated_deps[key] = deps[key].filter(x => !next_group.includes(x));
          }
        });
        deps = updated_deps;
      }
      return sorted;
    }

    function box_to_python(box) {

      function quoteParamValue(value) {
        if (/\n/g.test(value)) {
          return `'''${value}'''`;
        } else {
          return `'${value}'`;
        }
      }

      function paramsToStr(params, isParametric) {
        var paramStrings = [];
        Object.keys(params).forEach(function(key) {
          var quoted = quoteParamValue(params[key]);
          if (isParametric) {
            paramStrings.push(`${key}=pp(${quoted})`);
          } else {
            paramStrings.push(`${key}=${quoted}`);
          }
        });

        return paramStrings.join(', ');
      }

      function parameters() {
        return paramsToStr(box.parameters, false);
      }

      function parametricParameters() {
        return paramsToStr(box.parametricParameters, true);
      }

      function inputs() {
        // TODO: handle multioutput boxes
        return box.inputPlugs.map(x => `${box.inputs[x].boxId}`).join(', ');
      }

      var i = inputs();
      var p = parameters();
      var pp = parametricParameters();
      var args = [i, p, pp].filter(x => x);
      var code = `${box.id} = lk.${box.pythonOp}(${args.join(', ')})`;
      return code;
    }

    // Custom boxes are not supported
    // TODO: assert for selected custom boxes
    var boxes = selectedBoxIds.map(x => workspace.getBox(x));
    var prepared = boxes.map(x => ({
      id: snakeCase(x.instance.id),
      op: x.instance.operationId,
      pythonOp: pythonName(x.instance.operationId),
      parameters: x.instance.parameters,
      parametricParameters: x.instance.parametricParameters,
      inputs: snakeInputs(x.instance.inputs),
      inputPlugs: x.metadata.inputs,
      category: x.metadata.categoryId})
    );
    removeNonSelectedInputs(prepared);
    var unconnectedInputs = unconnected(prepared);
    var boxesToGenerate = unconnectedInputs.concat(prepared);
    var deps = dependencies(boxesToGenerate);
    var boxMap = mapping(boxesToGenerate);
    var sorted = topsort(deps);

    var generatedCode = [];
    sorted.forEach(function(boxId) {
      var box = boxMap[boxId];
      generatedCode.push(box_to_python(box));
    });
    var result = generatedCode.join('\n');

    /* eslint-disable no-console */
    console.log(result);

  };

  return pythonUtil;
});
