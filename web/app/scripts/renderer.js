'use strict';

angular.module('biggraph').directive('renderer', function($timeout) {
  /* global THREE */
  return {
    restrict: 'E',
    link: function(scope, element) {
      // Position the renderer as set by our creator.
      element.width(scope.width);
      element.css({ left: scope.left });

      // Wait for layout.
      $timeout(function() {
        // Create the canvas.
        var three = THREE.Bootstrap({
          element: element[0],
          plugins: ['core', 'controls', 'cursor'],
          controls: {
            klass: THREE.OrbitControls,
          },
          camera: { fov: 90 },
          renderer: {
            parameters: {
              alpha: true,
              antialias: true,
            },
          },
        });

        // Stop auto-rotate on mousedown.
        three.element.addEventListener('mousedown', function() {
          three.controls.autoRotate = false;
        });

        // Clean up when the directive is destroyed.
        scope.$on('$destroy', function() {
          if (three) {
            three.destroy();
          }
        });

        // Set basic scene.
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

        // Build the scene from the given edges.
        function plot(edges) {
          clear();
          // Geometry generation. 4 points and 2 triangles are generated for each edge.
          var n = edges.length;
          // Position of this point.
          var ps = new Float32Array(n * 4 * 3);
          // Index array.
          var is = new Uint32Array(n * 6);
          for (var i = 0; i < n; ++i) {
            var src = edges[i].aPos;
            var dst = edges[i].bPos;
            // The more edges we have, the thinner we make them.
            var w = Math.min(0.4, edges[i].size * 100 / n);
            ps[4 * 3 * i + 0] = src.x - w; ps[4 * 3 * i + 3] = src.x + w;
            ps[4 * 3 * i + 1] = ps[4 * 3 * i + 4] = src.y;
            ps[4 * 3 * i + 2] = ps[4 * 3 * i + 5] = src.z;
            ps[4 * 3 * i + 6] = dst.x - w; ps[4 * 3 * i + 9] = dst.x + w;
            ps[4 * 3 * i + 7] = ps[4 * 3 * i + 10] = dst.y;
            ps[4 * 3 * i + 8] = ps[4 * 3 * i + 11] = dst.z;
            is[6 * i + 0] = 4 * i + 0;
            is[6 * i + 1] = 4 * i + 1;
            is[6 * i + 2] = 4 * i + 2;
            is[6 * i + 3] = 4 * i + 2;
            is[6 * i + 4] = 4 * i + 1;
            is[6 * i + 5] = 4 * i + 3;
          }

          var geom = new THREE.BufferGeometry();
          geom.addAttribute('index', new THREE.BufferAttribute(is, 1));
          geom.addAttribute('position', new THREE.BufferAttribute(ps, 3));
          geom.computeVertexNormals();
          var mat = new THREE.MeshPhongMaterial({
            color: 0x807050,
            specular: 0xffffff,
            shading: THREE.FlatShading,
          });
          three.scene.add(new THREE.Mesh(geom, mat));
        }

        plot(scope.edges);
      });
    },
  };
});
