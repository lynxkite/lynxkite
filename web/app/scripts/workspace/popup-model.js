'use strict';

angular.module('biggraph').factory('PopupModel', function() {
  // Creates a new popup model data structure.
  // id: unique key
  // content: description of content to render
  // x, y, width, height: location and size of the popup
  // owner: owning object. It should have an owner.popups
  //        list of all popups. And owner.movedPopup a pointer
  //        to the currently moved popup, if any.
  var PopupModel = function(id, title, content, x, y, width, height, owner) {
    this.id = id;
    this.title = title;
    this.content = content;
    this.x = x;
    this.y = y;
    this.width = width;
    this.height = height;
    this.owner = owner;
  };

  PopupModel.prototype.onMouseDown = function(event) {
    var leftButton = event.buttons & 1;
    if (leftButton) {
      event.stopPropagation();
      this.owner.movedPopup = this;
      this.moveOffsetX = this.x - event.pageX;
      this.moveOffsetY = this.y - event.pageY;
    }
  };

  PopupModel.prototype.onMouseUp = function() {
    this.owner.movedPopup = undefined;
  };

  PopupModel.prototype.onMouseMove = function(event) {
    var leftButton = event.buttons & 1;
    if (!leftButton) {
      // Button is no longer pressed. (It was released outside of the window, for example.)
      this.owner.movedPopup = undefined;
    } else {
      this.x = this.moveOffsetX + event.pageX;
      this.y = this.moveOffsetY + event.pageY;
    }
  };

  PopupModel.prototype.isOpen = function() {
    var that = this;
    return this.owner.popups.find(function(p) { return p.id === that.id; }) !== undefined;
  };

  PopupModel.prototype.close = function() {
    var that = this;
    this.owner.popups = this.owner.popups.filter(function(p) { return p.id !== that.id; });
  };

  PopupModel.prototype.open = function() {
    if (!this.isOpen()) {
      this.owner.popups.push(this);
    }
  };

  PopupModel.prototype.toggle = function() {
    if (this.isOpen()) {
      this.close();
    } else {
      this.open();
    }
  };

  return PopupModel;
});
