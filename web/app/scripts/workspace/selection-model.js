'use strict';

angular.module('biggraph').factory('SelectionModel', function() {
  var SelectionModel = function() {
    this.startX = undefined;
    this.startY = undefined;
    this.endX = undefined;
    this.endY = undefined;
    // The parameters below are calculated from the above ones by this.updateSelection.
    this.leftX = undefined;
    this.upperY = undefined;
    this.width = undefined;
    this.height = undefined;
  };

  SelectionModel.prototype.update = function() {
    this.leftX = Math.min(this.startX, this.endX);
    this.upperY = Math.min(this.startY, this.endY);
    this.width = Math.abs(this.endX - this.startX);
    this.height = Math.abs(this.endY - this.startY);
  };

  SelectionModel.prototype.remove = function() {
    this.startX = undefined;
    this.endX = undefined;
    this.startY = undefined;
    this.endY = undefined;
    this.leftX = undefined;
    this.upperY = undefined;
    this.width = undefined;
    this.length = undefined;
  };

  SelectionModel.prototype.inSelection = function(box) {
    return (this.leftX < box.instance.x + box.width &&
      box.instance.x < this.leftX + this.width &&
      this.upperY < box.instance.y + box.height &&
      box.instance.y < this.upperY + this.height);
  };

  SelectionModel.prototype.onMouseDown = function(event) {
    this.endX = event.logicalX;
    this.endY = event.logicalY;
    this.startX = event.logicalX;
    this.startY = event.logicalY;
    this.update();
  };

  SelectionModel.prototype.onMouseMove = function(event) {
    this.endX = event.logicalX;
    this.endY = event.logicalY;
    this.update();
  };

  return SelectionModel;
});
