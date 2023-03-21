// Generates Python API code for selected boxes or for the whole workspace.
import '../app';
import pythonCodeTemplate from '../python-code.html?url';

angular.module('biggraph').factory('pythonCodeGenerator', ['$uibModal', function($uibModal) {

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
      const dummyInputs = [];
      for (let box of boxes) {
        for (let plug of box.inputPlugs) {
          if (!box.inputs[plug]) {
            const inputId = 'input_' + plug + '_for_' + box.id;
            // Now we connect it to a dummy input.
            box.inputs[plug] = {boxId: inputId, id: 'input'};
            dummyInputs.push(inputId);
          }
        }
      }
      return dummyInputs;
    }

    function dependencies(boxes, dummyInputs) {
      const deps = {};
      for (let box of boxes) {
        const fromId = box.id;
        deps[fromId] = Object.values(box.inputs).filter(x => !dummyInputs.includes(x.boxId)).map(x => x.boxId);
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
        function addSlashes(str) {
          return (str + '').replace(/[\\"']/g, '\\$&').replace(/\0/g, '\\0');
        }

        if (/\n/g.test(value)) {
          return `'''${addSlashes(value)}'''`;
        } else {
          return `'${addSlashes(value)}'`;
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
      const importBoxNames = [
        'importCSV', 'importJSON', 'importFromHive',
        'importParquet', 'importORC', 'importJDBC'];
      if (!importBoxNames.includes(box.pythonOp)) {
        return code;
      } else { // Add comment about import operations
        const explanation = '# Import boxes have to be handled separately\n';
        const commented = '# ' + code.replace(/\n/g, '\n# ');
        return explanation + commented;
      }
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
    const dummyInputs = unconnected(prepared);
    const deps = dependencies(prepared, dummyInputs);
    const boxMap = mapping(prepared);
    const sorted = topsort(deps);

    const generatedCode = [];
    for (let boxId of sorted) {
      if (boxId !== 'anchor') {
        generatedCode.push(boxToPython(boxMap[boxId], boxMap));
      }
    }
    const pythonCode = generatedCode.join('\n');

    $uibModal.open({
      templateUrl: pythonCodeTemplate,
      controller: 'PythonCodeCtrl',
      resolve: { code: function() { return pythonCode; } },
      size: 'lg',
    }).result.then(function() {
      // Ignore value.
    }, function() {
      // Ignore errors.
    });
  }
  return { saveAsPython };
}]);
