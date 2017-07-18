'use strict';

angular.module('biggraph').factory('PopupModel', function(environment) {
  // Creates a new popup model data structure.
  // id: Unique key.
  // content: Description of content to render.
  // x, y, width, height: Location and size of the popup.
  // owner: Owning object. It should have an owner.popups
  //        list of all popups. And owner.startMovingPopup(p) to route onMouseMove() calls.
  function PopupModel(id, title, content, x, y, width, height, owner) {
    this.id = id;
    this.title = title;
    this.content = content;
    this.x = x;
    this.y = y;
    this.width = width;
    this.height = height;
    this.owner = owner;
    this.element = undefined;
  }

  PopupModel.prototype.updateSize = function() {
    var popupElement = this.element.find('.popup-content')[0];
    // Save width and height of the popup. Remove 'px' from the end.
    this.width = parseInt(popupElement.style.width.slice(0, -2));
    this.height = parseInt(popupElement.style.height.slice(0, -2));
  };

  PopupModel.prototype.onMouseDown = function(event) {
    var leftButton = event.buttons & 1;
    // Protractor omits button data from simulated mouse events.
    if (leftButton) {
      this.owner.startMovingPopup(this);
      this.moveOffsetX = this.x - event.pageX;
      this.moveOffsetY = this.y - event.pageY;
    }
  };

  PopupModel.prototype.onMouseMove = function(event) {
    var leftButton = event.buttons & 1;
    // Protractor omits button data from simulated mouse events.
    if (leftButton || environment.protractor) {
      // Only move the popup if we are in the 'moving mode' (i.e. movedPopup is defined).
      if (this.owner.movedPopup === this) {
        this.x = this.moveOffsetX + event.pageX;
        this.y = this.moveOffsetY + event.pageY;
      }
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

  PopupModel.prototype.bringToFront = function() {
    var leftButton = event.buttons & 1;
    // Protractor omits button data from simulated mouse events.
    if (leftButton || environment.protractor) {
      this.close();
      this.open();
    }
  };

  // Returns a reference to the object in the workspace this popup belongs to.
  PopupModel.prototype.contentObject = function(workspace) {
    if (this.content.type === 'box') {
      return workspace.getBox(this.content.boxId);
    } else if (this.content.type === 'plug') {
      return workspace.getOutputPlug(this.content.boxId, this.content.plugId);
    }
  };

  // Computes the triangle for the popup trail as a string.
  PopupModel.prototype.trail = function(pageToLogical, logicalToPage, workspace) {
    // "L" variables are in logical coordinates, P variables are in page coordinates.
    var anchor = this.contentObject(workspace);
    var anchorL = {
      x: anchor.cx(),
      y: anchor.cy() };
    var anchorP = logicalToPage(anchorL);
    var attachP = {
      x: this.x + this.width / 2,
      y: this.y + 20 };
    // We want an isosceles triangle with a fixed width at the attachment point. Like this:
    //
    //     Attach
    // A +---+---+ B
    //    \     /
    //     \   /
    //      \ /
    //       + Anchor
    //
    var HALF_WIDTH = 10;
    var dx = attachP.x - anchorP.x;
    var dy = attachP.y - anchorP.y;
    var d = Math.sqrt(dx * dx + dy * dy);
    // Normalized direction.
    if (d === 0) { return; }
    var nx = dx / d;
    var ny = dy / d;
    // Orthogonal points A and B.
    var aP = {
      x: attachP.x + ny * HALF_WIDTH,
      y: attachP.y - nx * HALF_WIDTH };
    var aL = pageToLogical(aP);
    var bP = {
      x: attachP.x - ny * HALF_WIDTH,
      y: attachP.y + nx * HALF_WIDTH };
    var bL = pageToLogical(bP);
    return anchorL.x + ',' + anchorL.y + ' ' + aL.x + ',' + aL.y + ' ' + bL.x + ',' + bL.y;
  };

  return PopupModel;
});
