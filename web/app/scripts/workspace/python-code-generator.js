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
      let snakeInps = {};
      for (let key of Object.keys(inputs)) {
        snakeInps[key] = snakeInput(inputs[key]);
      }
      return snakeInps;
    }

    function pythonName(opId) {
      const cleaned = opId.replace(/[^\w_\s]+/g, '').split(' ');
      const head = cleaned[0];
      const tail = cleaned.slice(1);
      return head.toLowerCase() + tail.map(x => x[0].toUpperCase() + x.slice(1)).join('');
    }

    function removeNonSelectedInputs(boxes) {
      const selected = boxes.map(x => x.id);
      for (let box of boxes) {
        for (let key of Object.keys(box.inputs)) {
          if (!selected.includes(box.inputs[key].boxId)) {
            delete box.inputs[key];
          }
        }
      }
    }

    function unconnected(boxes) {
      let inputBoxes = [];
      for (let box of boxes) {
        for (let plug of box.inputPlugs) {
          if (!box.inputs[plug]) {
            const input_id = 'input_' + plug + '_for_' + box.id;
            // Now we connect it to a dummy input.
            box.inputs[plug] = {boxId: input_id, id: 'input'};
            inputBoxes.push({
              id: input_id,
              op: 'Input',
              pythonOp: 'input',
              parameters: {name: input_id},
              parametricParameters: {},
              inputs: {},
              inputPlugs: [],
              category: 'Workflow'});
          }
        }
      }
      return inputBoxes;
    }

    function dependencies(boxes) {
      let deps = {};
      for (let box of boxes) {
        const fromId = box.id;
        deps[fromId] = Object.values(box.inputs).map(b => b.boxId);
      }
      return deps;
    }

    function mapping(boxes) {
      let map = {};
      for (let box of boxes) {
        map[box.id] = box;
      }
      return map;
    }

    function topsort(deps) {
      let sorted = [];
      /* eslint-disable no-constant-condition */
      while (true) {
        const next_group = Object.keys(deps).filter(x => deps[x].length === 0);
        if (next_group.length === 0) {
          break;
        }
        sorted.push.apply(sorted, next_group); // extend
        let updated_deps = {};
        for (let key of Object.keys(deps)) {
          if (!next_group.includes(key)) {
            updated_deps[key] = deps[key].filter(x => !next_group.includes(x));
          }
        }
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
        let paramStrings = [];
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
        // TODO: handle multioutput boxes
        return box.inputPlugs.map(x => `${box.inputs[x].boxId}`).join(', ');
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
    let prepared = boxes.map(x => ({
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
    const unconnectedInputs = unconnected(prepared);
    const boxesToGenerate = unconnectedInputs.concat(prepared);
    const deps = dependencies(boxesToGenerate);
    const boxMap = mapping(boxesToGenerate);
    const sorted = topsort(deps);

    let generatedCode = [];
    for (let boxId of sorted) {
      generatedCode.push(box_to_python(boxMap[boxId]));
    }
    const result = generatedCode.join('\n');

    /* eslint-disable no-console */
    console.log(result);

  };

  return pythonUtil;
});
