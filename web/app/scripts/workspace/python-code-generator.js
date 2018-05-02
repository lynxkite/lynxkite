// Generates Python API code for selected boxes or for the whole workspace.
'use strict';

angular.module('biggraph').factory('pythonCodeGenerator', function($modal) {

  function saveAsPython (workspace, selectedBoxIds) {

    function snakeCase(boxId) {
      return boxId.toLowerCase().replace(/-/g, '_');
    }

    function snakeInput(inp) {
      return {
        boxId: snakeCase(inp.boxId),
        id: inp.id};
    }

    function snakeInputs(inputs) {
      const snakeInps = {};
      for (let key in inputs) {
        snakeInps[key] = snakeInput(inputs[key]);
      }
      return snakeInps;
    }

    function pythonName(opId) {
      // This has to match with lynx.kite._python_name()
      const cleaned = opId.replace(/[^\w_\s]+/g, '').split(' ');
      const head = cleaned[0];
      const tail = cleaned.slice(1);
      return head.toLowerCase() + tail.map(x => x[0].toUpperCase() + x.slice(1)).join('');
    }

    function removeNonSelectedInputs(boxes) {
      const selected = boxes.map(x => x.id);
      for (let box of boxes) {
        for (let key in box.inputs) {
          if (!selected.includes(box.inputs[key].boxId)) {
            delete box.inputs[key];
          }
        }
      }
    }

    function unconnected(boxes) {
      const inputBoxes = [];
      for (let box of boxes) {
        for (let plug of box.inputPlugs) {
          if (!box.inputs[plug]) {
            const inputId = 'input_' + plug + '_for_' + box.id;
            // Now we connect it to a dummy input.
            box.inputs[plug] = {boxId: inputId, id: 'input'};
            inputBoxes.push({
              id: inputId,
              op: 'Input',
              pythonOp: 'input',
              parameters: {name: inputId},
              parametricParameters: {},
              inputs: {},
              inputPlugs: [],
              outputPlugs: ['input'],
              category: 'Workflow'});
          }
        }
      }
      return inputBoxes;
    }

    function dependencies(boxes) {
      const deps = {};
      for (let box of boxes) {
        const fromId = box.id;
        deps[fromId] = Object.values(box.inputs).map(b => b.boxId);
      }
      return deps;
    }

    function mapping(boxes) {
      const map = {};
      for (let box of boxes) {
        map[box.id] = box;
      }
      return map;
    }

    function topsort(deps) {
      const sorted = [];
      /* eslint-disable no-constant-condition */
      while (true) {
        const nextGroup = Object.keys(deps).filter(x => deps[x].length === 0);
        if (nextGroup.length === 0) {
          break;
        }
        sorted.push.apply(sorted, nextGroup); // extend
        const updatedDeps = {};
        for (let key in deps) {
          if (!nextGroup.includes(key)) {
            updatedDeps[key] = deps[key].filter(x => !nextGroup.includes(x));
          }
        }
        deps = updatedDeps;
      }
      return sorted;
    }

    function boxToPython(box, boxMap) {

      function quoteParamValue(value) {
        if (/\n/g.test(value)) {
          return `'''${value}'''`;
        } else {
          return `'${value}'`;
        }
      }

      function paramsToStr(params, isParametric) {
        const paramStrings = [];
        for (let key of Object.keys(params)) {
          const quoted = quoteParamValue(params[key]);
          if (isParametric) {
            paramStrings.push(`${key}=pp(${quoted})`);
          } else {
            paramStrings.push(`${key}=${quoted}`);
          }
        }
        return paramStrings.join(', ');
      }

      function parameters() {
        return paramsToStr(box.parameters, false);
      }

      function parametricParameters() {
        return paramsToStr(box.parametricParameters, true);
      }

      function inputs() {
        return box.inputPlugs.map(function(x) {
          const inputBox = box.inputs[x].boxId;
          const outputPlugOfInput = box.inputs[x].id;
          if (boxMap[inputBox] && boxMap[inputBox].outputPlugs.length > 1) {
            return `${inputBox}['${outputPlugOfInput}']`;
          } else {
            return `${inputBox}`;
          }
        }).join(', ');
      }

      const i = inputs();
      const p = parameters();
      const pp = parametricParameters();
      const args = [i, p, pp].filter(x => x);
      const code = `${box.id} = lk.${box.pythonOp}(${args.join(', ')})`;
      return code;
    }

    // Custom boxes are not supported
    // TODO: assert for selected custom boxes
    const boxes = selectedBoxIds.map(x => workspace.getBox(x));
    const prepared = boxes.map(x => ({
      id: snakeCase(x.instance.id),
      op: x.instance.operationId,
      pythonOp: pythonName(x.instance.operationId),
      parameters: x.instance.parameters,
      parametricParameters: x.instance.parametricParameters,
      inputs: snakeInputs(x.instance.inputs),
      inputPlugs: x.metadata.inputs,
      outputPlugs: x.metadata.outputs,
      category: x.metadata.categoryId})
    );
    removeNonSelectedInputs(prepared);
    const unconnectedInputs = unconnected(prepared);
    const boxesToGenerate = unconnectedInputs.concat(prepared);
    const deps = dependencies(boxesToGenerate);
    const boxMap = mapping(boxesToGenerate);
    const sorted = topsort(deps);

    const generatedCode = [];
    for (let boxId of sorted) {
      if (boxId !== 'anchor') {
        generatedCode.push(boxToPython(boxMap[boxId], boxMap));
      }
    }
    const pythonCode = generatedCode.join('\n');

    $modal.open({
      templateUrl: 'scripts/python-code.html',
      controller: 'PythonCodeCtrl',
      resolve: { code: function() { return pythonCode; } },
      animation: false,  // Protractor does not like the animation.
      size: 'lg',
    }).result.then(function() {}, function() {});


  }

  return { saveAsPython };
});
