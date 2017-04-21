'use strict';

angular.module('biggraph').factory('popupModel', function() {
  // Creates a new popup model data structure.
  // id: unique key
  // content: description of content to render
  // x, y, width, height: location and size of the popup
  // owner: owning object. It should have an owner.popups
  //        list of all popups. And owner.movedPopup a pointer
  //        to the currently moved popup, if any.
  return function(id, content, x, y, width, height, owner) {
    return {
      id: id,
      content: content,
      x: x,
      y: y,
      width: width,
      height: height,
      onMouseDown: function(event) {
        event.stopPropagation();
        owner.movedPopup = this;
        this.moveOffsetX = this.x - event.pageX;
        this.moveOffsetY = this.y - event.pageY;
      },
      onMouseUp: function() {
        owner.movedPopup = undefined;
      },
      onMouseMove: function(event) {
        this.x = this.moveOffsetX + event.pageX;
        this.y = this.moveOffsetY + event.pageY;
      },
      close: function() {
        for (var i = 0; i < owner.popups.length; ++i) {
          if (owner.popups[i].id === id) {
            owner.popups.splice(i, 1);
            return true;
          }
        }
        return false;
      },
      // Close if opened, open if closed.
      toggle: function() {
        if (!this.close()) {
          owner.popups.push(this);
        }
      },
    };
  };
});
