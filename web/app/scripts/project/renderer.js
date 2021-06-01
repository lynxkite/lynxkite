// 3D graph visualization.
'use strict';

angular.module('biggraph').directive('renderer', function($timeout) {
  /* global THREE */
  return {
    restrict: 'E',
    link: function(scope, element) {
      // Position the renderer as set by our creator.
      element.width(scope.width);
      element.css({ left: scope.left });
      let disposed;

      // Wait for layout.
      $timeout(function() {
        // Create the canvas.
        const W = element.width();
        const H = element.height();
        const camera = new THREE.PerspectiveCamera(50, W / H, 0.1, 10000);
        const renderer = new THREE.WebGLRenderer({ alpha: true, antialias: true });
        const controls = new THREE.OrbitControls(camera, renderer.domElement);
        controls.autoRotate = true;
        controls.autoRotateSpeed = 2.0;
        camera.position.set(10, 5, 120);
        controls.update();
        renderer.setSize(W, H);
        element.append(renderer.domElement);

        // Stop auto-rotate on mousedown.
        element.mousedown(function() {
          controls.autoRotate = false;
        });

        // Clean up when the directive is destroyed.
        scope.$on('$destroy', function() {
          renderer.dispose();
          disposed = true;
        });

        // Set basic scene.
        function buildScene(edges, layout3D) {
          const scene = new THREE.Scene();
          const hemiLight = new THREE.HemisphereLight(0xffffff, 0xffffff, 0.6);
          hemiLight.position.set(0, 500, 0);
          scene.add(hemiLight);
          const dirLight = new THREE.DirectionalLight(0xffffff, 1);
          dirLight.position.set(-1, 1.75, 1);
          scene.add(dirLight);
          const dirLight2 = new THREE.DirectionalLight(0xffffff, 0.5);
          dirLight2.position.set(-2, -1.75, -1);
          scene.add(dirLight2);

          // Build the scene from the given edges.
          // Geometry generation. 8 points and 12 triangles are generated for each edge.
          const n = edges.length;
          // Position of this point.
          const ps = new Float32Array(n * 8 * 3 * 3);
          // Index array.
          const is = new Uint32Array(n * 12 * 3);
          for (let i = 0; i < n; ++i) {
            if (edges[i].a === edges[i].b) { continue; }
            const src = layout3D[edges[i].a];
            const dst = layout3D[edges[i].b];
            // The more edges we have, the thinner we make them.
            const w = Math.min(0.4, edges[i].size * 100 / n);
            addRod(ps, is, i, src, dst, w);
          }

          const geom = new THREE.BufferGeometry();
          geom.setIndex(new THREE.BufferAttribute(is, 1));
          geom.addAttribute('position', new THREE.BufferAttribute(ps, 3));
          geom.computeVertexNormals();
          const mat = new THREE.MeshPhongMaterial({
            color: 0x807050,
            specular: 0xffffff,
            flatShading: true,
          });
          scene.add(new THREE.Mesh(geom, mat));
          return scene;
        }

        function addRod(ps, is, i, a, b, w) {
          const j = { x: b.x - a.x, y: b.y - a.y, z: b.z - a.z };
          const ortho = orthogonals(j);
          const h = ortho[0], v = ortho[1];
          h.x *= w; h.y *= w; h.z *= w;
          v.x *= w; v.y *= w; v.z *= w;
          const pp = i * 8 * 3;
          // Normals are stored per-vertex. Because each vertex is used in 3 faces, they would need
          // to have 3 different normals. So we create 3 copies of each vertex, and use different
          // copies for each face.
          for (let d = 0; d < 3; ++d) {
            const r = pp + 8 * d;
            addPoint(ps, r + 0, { x: a.x + h.x, y: a.y + h.y, z: a.z + h.z });
            addPoint(ps, r + 1, { x: a.x - h.x, y: a.y - h.y, z: a.z - h.z });
            addPoint(ps, r + 2, { x: a.x + v.x, y: a.y + v.y, z: a.z + v.z });
            addPoint(ps, r + 3, { x: a.x - v.x, y: a.y - v.y, z: a.z - v.z });
            addPoint(ps, r + 4, { x: b.x + h.x, y: b.y + h.y, z: b.z + h.z });
            addPoint(ps, r + 5, { x: b.x - h.x, y: b.y - h.y, z: b.z - h.z });
            addPoint(ps, r + 6, { x: b.x + v.x, y: b.y + v.y, z: b.z + v.z });
            addPoint(ps, r + 7, { x: b.x - v.x, y: b.y - v.y, z: b.z - v.z });
          }
          // r1, r2, r3 are the 3 copies of the vertices. Each index is used once from each copy.
          const ii = i * 6, r1 = pp, r2 = pp + 8, r3 = pp + 16;
          addQuad(is, ii + 0, r1 + 0, r1 + 3, r1 + 1, r1 + 2);
          addQuad(is, ii + 1, r2 + 0, r2 + 2, r1 + 6, r1 + 4);
          addQuad(is, ii + 2, r3 + 0, r2 + 4, r1 + 7, r2 + 3);
          addQuad(is, ii + 3, r1 + 5, r2 + 7, r3 + 4, r2 + 6);
          addQuad(is, ii + 4, r2 + 5, r3 + 6, r3 + 2, r2 + 1);
          addQuad(is, ii + 5, r3 + 5, r3 + 1, r3 + 3, r3 + 7);
        }

        function orthogonals(j) {
          const h = { x: j.y * j.z, y: -0.5 * j.x * j.z, z: -0.5 * j.x * j.y };
          const v = {
            x: j.y * h.z - j.z * h.y,
            y: j.z * h.x - j.x * h.z,
            z: j.x * h.y - j.y * h.x };
          return [normalized(h), normalized(v)];
        }

        function normalized(j) {
          const d = Math.sqrt(j.x * j.x + j.y * j.y + j.z * j.z);
          return { x: j.x / d, y: j.y / d, z: j.z / d };
        }

        function addPoint(ps, i, p) {
          ps[3 * i + 0] = p.x;
          ps[3 * i + 1] = p.y;
          ps[3 * i + 2] = p.z;
        }

        function addQuad(is, i, p1, p2, p3, p4) {
          is[6 * i + 0] = p1;
          is[6 * i + 1] = p2;
          is[6 * i + 2] = p3;
          is[6 * i + 3] = p1;
          is[6 * i + 4] = p3;
          is[6 * i + 5] = p4;
        }

        function animate() {
          if (disposed) { return; }
          requestAnimationFrame(animate);
          controls.update();
          renderer.render(scene, camera);
        }

        const scene = buildScene(scope.edges, scope.layout3D);
        animate();
      });
    },
  };
});
