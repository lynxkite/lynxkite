'use strict';

angular.module('biggraph').directive('globalView', function() {
  /* global THREE */
  return {
    restrict: 'E',
    scope: { graph: '=' },
    templateUrl: 'global-view.html',
    link: function(scope, element) {
      function updateGraph() {
        if (scope.graph.view === undefined || !scope.graph.view.$resolved) {
          loading();
        } else if (scope.graph.view.$error) {
          scope.graph.view.$popupError();
          error();
        } else {
          update(scope.graph.view);
        }
      }
      scope.$watch('graph.view', updateGraph);
      scope.$watch('graph.view.$resolved', updateGraph);

      var three = THREE.Bootstrap({
        element: element[0],
        plugins: ['core', 'controls', 'cursor'],
        controls: {
          klass: THREE.OrbitControls,
        },
        camera: { fov: 90 },
      });
      three.renderer.setClearColor(0x222222);
      three.element.addEventListener('mousedown', function() {
        three.controls.autoRotate = false;
      });

      scope.$on('$destroy', function() {
        if (three) {
          three.destroy();
        }
      });
      function clear() {
        three.controls.autoRotate = true;
        three.controls.autoRotateSpeed = 2.0;

        three.scene = new THREE.Scene();
        three.camera.position.set(10, 5, 12);

        var hemiLight = new THREE.HemisphereLight(0xffffff, 0xffffff, 0.6);
        hemiLight.position.set(0, 500, 0);
        three.scene.add(hemiLight);
        var dirLight = new THREE.DirectionalLight(0xffffff, 1);
        dirLight.position.set(-1, 1.75, 1);
        three.scene.add(dirLight);
        var dirLight2 = new THREE.DirectionalLight(0xffffff, 0.5);
        dirLight2.position.set(-2, -1.75, -1);
        three.scene.add(dirLight2);
      }

      function loading() {
        clear();
        three.controls.autoRotateSpeed = 5.0;
        var geom = new THREE.DodecahedronGeometry(10, 1);
        var mat = new THREE.MeshPhongMaterial({
          color: 0x809080,
          specular: 0xffffff,
          shading: THREE.FlatShading,
        });
        three.scene.add(new THREE.Mesh(geom, mat));
      }

      function error() {
        clear();
        three.controls.autoRotateSpeed = 20.0;
        var S = 5;
        var dot = new THREE.BoxGeometry(S, S, S);
        var geom = new THREE.BoxGeometry(S, 3 * S, S);
        geom.merge(dot, new THREE.Matrix4().makeTranslation(0, -3 * S, 0), 0);
        var mat = new THREE.MeshPhongMaterial({
          color: 0x906060,
          specular: 0x808080,
          shading: THREE.FlatShading,
        });
        var mesh = new THREE.Mesh(geom, mat);
        mesh.position.y = 0.5 * S;
        three.scene.add(mesh);
      }

      function update(data) {
        clear();
        // Geometry generation. 4 points and 2 triangles are generated for each edge.
        var n = data.edges.length;
        // Position of this point.
        var ps = new Float32Array(n * 4 * 3);
        // Destination from this point. (The other end of the edge.)
        var ds = new Float32Array(n * 4 * 3);
        // Side. Offset this much to provide a line width.
        var ss = new Float32Array(n * 4);
        // Index array.
        var is = new Uint32Array(n * 6);
        for (var i = 0; i < n; ++i) {
          var src = data.vertices[data.edges[i].a];
          var dst = data.vertices[data.edges[i].b];
          ps[4 * 3 * i + 0] = ps[4 * 3 * i + 3] = ds[4 * 3 * i + 6] = ds[4 * 3 * i + 9] = src.x;
          ps[4 * 3 * i + 1] = ps[4 * 3 * i + 4] = ds[4 * 3 * i + 7] = ds[4 * 3 * i + 10] = src.y;
          ps[4 * 3 * i + 2] = ps[4 * 3 * i + 5] = ds[4 * 3 * i + 8] = ds[4 * 3 * i + 11] = src.z;
          ps[4 * 3 * i + 6] = ps[4 * 3 * i + 9] = ds[4 * 3 * i + 0] = ds[4 * 3 * i + 3] = dst.x;
          ps[4 * 3 * i + 7] = ps[4 * 3 * i + 10] = ds[4 * 3 * i + 1] = ds[4 * 3 * i + 4] = dst.y;
          ps[4 * 3 * i + 8] = ps[4 * 3 * i + 11] = ds[4 * 3 * i + 2] = ds[4 * 3 * i + 5] = dst.z;
          // The more edges we have, the thinner we make them.
          var w = Math.min(0.4, data.edges[i].size * 100 / n);
          ss[4 * i + 0] = ss[4 * i + 3] = w;
          ss[4 * i + 1] = ss[4 * i + 2] = -w;
          is[6 * i + 0] = 4 * i + 0;
          is[6 * i + 1] = 4 * i + 1;
          is[6 * i + 2] = 4 * i + 2;
          is[6 * i + 3] = 4 * i + 2;
          is[6 * i + 4] = 4 * i + 1;
          is[6 * i + 5] = 4 * i + 3;
        }

        var geom = new THREE.BufferGeometry();
        geom.addAttribute('index', new THREE.BufferAttribute(is, 1));
        geom.addAttribute('side', new THREE.BufferAttribute(ss, 1));
        geom.addAttribute('position', new THREE.BufferAttribute(ps, 3));
        geom.addAttribute('direction', new THREE.BufferAttribute(ds, 3));
        var mat = new THREE.ShaderMaterial({
          uniforms: { aspect: { type: 'f', value: three.camera.aspect } },
          attributes: { side: { type: 'f', value: ss }, direction: { type: 'f', value: ds } },
          vertexShader: element.find('#vertexShader').html(),
          fragmentShader: element.find('#fragmentShader').html(),
        });
        mat.side = THREE.DoubleSide;
        three.scene.add(new THREE.Mesh(geom, mat));
      }
    },
  };
});
