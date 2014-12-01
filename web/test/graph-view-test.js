var mocks = require('./mocks.js');
describe('the graph view', function() {
  beforeEach(function() {
    mocks.addTo(browser);
  });

  describe('in sampled view', function() {
    it('keeps the layout for the left side when opening the right side', function() {
      browser.get('/#/project/Project_Strawberry?q=%7B%22left%22:%7B%22projectName%22:%22Project_Strawberry%22,%22filters%22:%7B%22edge%22:%7B%7D,%22vertex%22:%7B%7D%7D,%22axisOptions%22:%7B%22edge%22:%7B%7D,%22vertex%22:%7B%7D%7D,%22graphMode%22:%22sampled%22,%22bucketCount%22:4,%22sampleRadius%22:1,%22animate%22:%7B%22enabled%22:false,%22labelAttraction%22:0%7D,%22attributeTitles%22:%7B%7D,%22centers%22:%5B%223333333%22,%224444444%22%5D%7D,%22right%22:%7B%22filters%22:%7B%22edge%22:%7B%7D,%22vertex%22:%7B%7D%7D,%22axisOptions%22:%7B%22edge%22:%7B%7D,%22vertex%22:%7B%7D%7D,%22graphMode%22:null,%22bucketCount%22:4,%22sampleRadius%22:1,%22animate%22:%7B%22enabled%22:false,%22labelAttraction%22:0%7D,%22attributeTitles%22:%7B%7D,%22centers%22:%5B%223333333%22,%224444444%22%5D,%22projectName%22:%22Project_Melon%22%7D%7D');
      var rightSideButton = element.all(by.css('.btn .glyphicon-eye-open')).last();
      function getPos(v) {
        return { x: v.getAttribute('cx'), y: v.getAttribute('cy') };
      }
      function getLength(arr) {
        return arr.length;
      }
      var positions = element.all(by.css('.vertex.sampled circle')).map(getPos);
      expect(positions.then(getLength)).toBe(6);  // The demo on the left.
      positions.then(function(leftSide) {
        rightSideButton.click();
        var positions = element.all(by.css('.vertex.sampled circle')).map(getPos);
        expect(positions.then(getLength)).toBe(12);  // The demo on both sides.
        // The left-side circles are still in the same place.
        for (var i = 0; i < leftSide.length; ++i) {
          expect(positions).toContain(leftSide[i]);
        }
      });
    });
  });
});
