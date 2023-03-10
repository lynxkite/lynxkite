// Visualization settings shared code.
import Drop from 'tether-drop';
import '../app';

angular.module('biggraph').factory('ViewSettings', function() {
  return function(scope, element) {
    this.drops = {};
    element.find('.token').each((i, e) => {
      if (!e.id) { return; }
      const menu = element.find('#menu-' + e.id);
      if (!menu.length) { return; }
      const drop = new Drop({
        target: e,
        content: menu[0],
        openOn: 'click',
        classes: 'drop-theme-menu',
        remove: true,
        position: 'bottom center',
        tetherOptions: {
          constraints: [{ to: 'window', attachment: 'together', pin: true, }],
        },
      });
      this.drops['menu-' + e.id] = drop;
    });

    this.getDrop = function(e) {
      e = e.currentTarget;
      // Try to find a parent that is a drop-down menu. Give up after 20 steps.
      for (let i = 0; i < 20; ++i) {
        if (this.drops[e.id] !== undefined) {
          return this.drops[e.id];
        }
        e = e.parentNode;
      }
    };

    scope.$on('$destroy', () => {
      for (const d of Object.values(this.drops)) {
        d.destroy();
      }
      this.drops = {};
    });
  };
});
