'use strict';

const lib = require('../test-lib.js');

module.exports = function(fw) {
  fw.transitionTest(
    undefined, 'ready for authentication tests',
    function() {
      browser.get('/#/');
      browser.executeScript(`
        window.sessionStorage.clear();
        window.localStorage.clear();
        window.localStorage.setItem('workspace-drawing-board tutorial done', 'true');
        window.localStorage.setItem('entry-selector tutorial done', 'true');
        `);
      browser.waitForAngular();
    }, function() {});

  fw.statePreservingTest(
    'ready for authentication tests', 'log in with invalid username and password',
    function() {
      browser.get('/#/login');
      $('#username').sendKeys('admin');
      $('#password').sendKeys('userpw');
      $('#login-button').click();
      expect(lib.error()).toEqual('Invalid username or password.');
      lib.closeErrors();
    });

  fw.transitionTest(
    'ready for authentication tests', 'logged in',
    function() {
      browser.get('/#/login');
      $('#username').sendKeys('admin');
      $('#password').sendKeys('adminpw');
      $('#login-button').click();
      expect($('#login-info').html === 'Logged in as admin');
    },
    function() {
    });
};
