'use strict';

const lastPositions = {}; // Keyed by ID so we can reopen the popups in their last locations.

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
    this.height = undefined;
    this.maxHeight = height;
    this.owner = owner;
    this.element = undefined;
    this.meta = false; // Whether the metadata editor is active.
    if (lastPositions[id]) {
      this.x = lastPositions[id].x;
      this.y = lastPositions[id].y;
      this.width = lastPositions[id].width;
      this.height = lastPositions[id].height;
    }
  }

  PopupModel.prototype.updateSize = function() {
    const popupElement = this.element.find('.popup-content')[0];
    // Save width and height of the popup. Remove 'px' from the end.
    this.width = parseInt(popupElement.style.width.slice(0, -2));
    const newHeight = parseInt(popupElement.style.height.slice(0, -2));
    if (newHeight) {
      this.height = newHeight;
      // max-height limits the initial automatic sizing. We unset it so manual sizing is unlimited.
      this.maxHeight = undefined;
    }
  };

  PopupModel.prototype.onMouseDown = function(event) {
    const leftButton = event.buttons & 1;
    // Protractor omits button data from simulated mouse events.
    if (leftButton) {
      this.owner.startMovingPopup(this);
      this.moveOffsetX = this.x - event.pageX;
      this.moveOffsetY = this.y - event.pageY;
    }
  };

  PopupModel.prototype.onMouseMove = function(event) {
    const leftButton = event.buttons & 1;
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
    const that = this;
    return this.owner.popups.find(function(p) { return p.id === that.id; }) !== undefined;
  };

  PopupModel.prototype.close = function() {
    const that = this;
    this.owner.popups = this.owner.popups.filter(function(p) { return p.id !== that.id; });
    lastPositions[this.id] = { x: this.x, y: this.y, width: this.width, height: this.height };
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

  PopupModel.prototype.bringToFront = function(event) {
    const leftButton = event.buttons & 1;
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
    // "L" constiables are in logical coordinates, P constiables are in page coordinates.
    const anchor = this.contentObject(workspace);
    if (!anchor) { return; }
    const anchorL = {
      x: anchor.cx(),
      y: anchor.cy() };
    const anchorP = logicalToPage(anchorL);
    const attachP = {
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
    const HALF_WIDTH = 10;
    const dx = attachP.x - anchorP.x;
    const dy = attachP.y - anchorP.y;
    const d = Math.sqrt(dx * dx + dy * dy);
    // Normalized direction.
    if (d === 0) { return; }
    const nx = dx / d;
    const ny = dy / d;
    // Orthogonal points A and B.
    const aP = {
      x: attachP.x + ny * HALF_WIDTH,
      y: attachP.y - nx * HALF_WIDTH };
    const aL = pageToLogical(aP);
    const bP = {
      x: attachP.x - ny * HALF_WIDTH,
      y: attachP.y + nx * HALF_WIDTH };
    const bL = pageToLogical(bP);
    return anchorL.x + ',' + anchorL.y + ' ' + aL.x + ',' + aL.y + ' ' + bL.x + ',' + bL.y;
  };

  PopupModel.prototype.toggleMeta = function() {
    this.meta = !this.meta;
  };

  return PopupModel;
});
