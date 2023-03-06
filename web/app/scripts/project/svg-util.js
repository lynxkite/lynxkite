// Utility functions for SVG building.
'use strict';

// JQuery addClass/removeClass does not work on SVG elements. (They are in
// another namespace, but the "class" attribute is in the default namespace.)
export function classesOf(e) {
  const l = e[0].getAttributeNS(null, 'class').split(' ');
  l.plus = function (cls) {
    if (l.indexOf(cls) === -1) {
      return l.concat(cls);
    } else {
      return l;
    }
  };
  l.minus = function (cls) {
    const i = l.indexOf(cls);
    if (i === -1) {
      return l;
    } else {
      return l.slice(0, i).concat(l.slice(i + 1));
    }
  };
  return l;
}

export function addClass(e, cls) {
  e[0].setAttributeNS(null, 'class', classesOf(e).plus(cls).join(' '));
}

export function removeClass(e, cls) {
  e[0].setAttributeNS(null, 'class', classesOf(e).minus(cls).join(' '));
}

export function arcParams(ax, ay, bx, by, zoom) {
  if (ax === bx && ay === by) {
    return { r: 0.1 * zoom, x: ax + 0.2 * zoom, y: ay };
  } else {
    const dx = bx - ax,
      dy = by - ay;
    const d = Math.sqrt(dx * dx + dy * dy);
    // Use larger radius (less curvature) for long distance edges.
    const r = (d * (d + 1000)) / 1000;
    const h = r - Math.sqrt(r * r - (d * d) / 4);
    return {
      r: r,
      x: ax + 0.5 * dx - (h * dy) / d,
      y: ay + 0.5 * dy + (h * dx) / d,
    };
  }
}

export function direction(ax, ay, bx, by) {
  const dx = bx - ax;
  const dy = by - ay;
  const d = Math.sqrt(dx * dx + dy * dy);
  if (d === 0) {
    return { x: 0, y: 1 };
  } else {
    return { x: dx / d, y: dy / d };
  }
}

export function arrows(ax, ay, bx, by, zoom, width) {
  const a = arcParams(ax, ay, bx, by, zoom);
  const d = direction(ax, ay, bx, by);
  d.x *= width;
  d.y *= width;
  const left = a.x + d.y - d.x + ' ' + (a.y - d.x - d.y);
  const right = a.x - d.y - d.x + ' ' + (a.y + d.x - d.y);
  const tip = a.x + d.x + ' ' + (a.y + d.y);
  function arc(x, y) {
    return `A ${a.r} ${a.r} 0 0 0 ${x} ${y}`;
  }
  const isLoop = ax === bx && ay === by;
  return {
    arc: isLoop ? `M ${ax} ${ay} ${arc(a.x, a.y)} ${arc(bx, by)}` : `M ${ax} ${ay} ${arc(bx, by)}`,
    arrow: `M ${tip} L ${left} L ${right} z`,
  };
}

export function group(l, attrs) {
  const g = create('g', attrs);
  g.append(l);
  return g;
}

export function create(tag, attrs) {
  const e = angular.element(document.createElementNS('http://www.w3.org/2000/svg', tag));
  if (attrs !== undefined) {
    for (const k in attrs) {
      e[0].setAttributeNS(null, k, attrs[k]);
    }
  }
  return e;
}
