var mocks = require('./mocks.js');
describe('the graph view', function() {
  beforeEach(function() {
    mocks.addTo(browser);
  });

  describe('in sampled view', function() {
    var sampledViewButtons = element.all(by.css('.project-name > .btn-group .glyphicon-eye-open'));
    function getPos(v) {
      return { x: v.getAttribute('cx'), y: v.getAttribute('cy') };
    }
    function getLength(arr) {
      return arr.length;
    }
    // Move all positions horizontally so that the x coordinate of the leftmost
    // position becomes zero.
    function normalizePositionsX(positions) {
      if (!positions) {
        return;
      }
      var minx = positions[0].x;
      for (var i = 1; i < positions.length; ++i) {
        minx = Math.min(minx, positions[i].x);
      }
      for (var i = 0; i < positions.length; ++i) {
        positions[i].x -= minx;
      }
    }
    // Round position coordinates to avoid failing for small numerical
    // differences.
    function roundCoordinates(positions) {
      for (var i = 0; i < positions.length; ++i) {
        positions[i].x = parseFloat(positions[i].x).toFixed(5);
        positions[i].y = parseFloat(positions[i].y).toFixed(5);
      }
    }

    it('keeps the layout for the left side (apart from horizontal shifting) when opening the right side', function() {
      browser.get('/#/project/Project_Strawberry?q=%7B%22left%22:%7B%22projectName%22:%22Project_Strawberry%22,%22filters%22:%7B%22edge%22:%7B%7D,%22vertex%22:%7B%7D%7D,%22axisOptions%22:%7B%22edge%22:%7B%7D,%22vertex%22:%7B%7D%7D,%22graphMode%22:%22sampled%22,%22bucketCount%22:4,%22sampleRadius%22:1,%22animate%22:%7B%22enabled%22:false,%22labelAttraction%22:0%7D,%22attributeTitles%22:%7B%7D,%22centers%22:%5B%223333333%22,%224444444%22%5D%7D,%22right%22:%7B%22filters%22:%7B%22edge%22:%7B%7D,%22vertex%22:%7B%7D%7D,%22axisOptions%22:%7B%22edge%22:%7B%7D,%22vertex%22:%7B%7D%7D,%22graphMode%22:null,%22bucketCount%22:4,%22sampleRadius%22:1,%22animate%22:%7B%22enabled%22:false,%22labelAttraction%22:0%7D,%22attributeTitles%22:%7B%7D,%22centers%22:%5B%223333333%22,%224444444%22%5D,%22projectName%22:%22Project_Melon%22%7D%7D');
      var rightSideButton = sampledViewButtons.last();
      var positions = element.all(by.css('.vertex.sampled circle')).map(getPos);
      expect(positions.then(getLength)).toBe(6);  // The demo on the left.
      positions.then(function(positionsBefore) {
        rightSideButton.click();
        var positions = element.all(by.css('.nodes.side0 .vertex.sampled circle')).map(getPos);
        expect(positions.then(getLength)).toBe(6);  // The demo on the left after the right side was opened.
        positions.then(function(positionsAfter) {
          normalizePositionsX(positionsBefore); roundCoordinates(positionsBefore);
          normalizePositionsX(positionsAfter); roundCoordinates(positionsAfter);
          // The left-side circles are still in the same place.
          for (var i = 0; i < positionsBefore.length; ++i) {
            expect(positionsAfter).toContain(positionsBefore[i]);
          }
        });
      });
    });

    function byText(text) {
      return by.xpath('.//*[contains(text(),\'' + text + '\')]');
    }
    it('keeps the layout when adding attributes', function() {
      browser.get('/#/project/Project_Strawberry');
      sampledViewButtons.click();
      var attr = element.all(by.css('[vertex-attribute]')).first();
      var dropDownToggle = attr.element(by.css('.sampled-visualizations .dropdown-toggle'))
      var asColor = attr.element(byText('Color'));
      var positions = element.all(by.css('.vertex.sampled circle')).map(getPos);
      expect(positions.then(getLength)).toBe(6);  // The demo graph.
      positions.then(function(original) {
        for (var n = 0; n < 2; ++n) {  // Turn color on and then off.
          dropDownToggle.click();
          asColor.click();
          var positions = element.all(by.css('.vertex.sampled circle')).map(getPos);
          expect(positions.then(getLength)).toBe(6);  // Same graph.
          // The circles are still in the same place.
          for (var i = 0; i < original.length; ++i) {
            expect(positions).toContain(original[i]);
          }
        }
      });
    });

    it('can open and close the context menu', function() {
      browser.get('/#/project/Project_Strawberry');
      sampledViewButtons.click();
      var menu = element(by.css('div.context-menu'));
      expect(menu.isDisplayed()).toBe(false);
      var circle = element.all(by.css('g.vertex > circle')).first();
      circle.click();
      expect(menu.isDisplayed()).toBe(true);
      var something = element.all(by.css('.side > .project > .project-name')).first();
      something.click();
      expect(menu.isDisplayed()).toBe(false);
    });
  });
});
