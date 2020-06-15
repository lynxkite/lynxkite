'use strict';

module.exports = function(fw) {
  fw.statePreservingTest(
    'logged in', 'change password',
    function() {
      function changePassword(oldPassword, newPassword) {
        $('.user-menu').click();
        $('.user-menu').element(by.linkText('change password')).click();
        $('#oldPassword').sendKeys(oldPassword);
        $('#newPassword').sendKeys(newPassword);
        $('#newPassword2').sendKeys(newPassword);
        $('#change-password-button').click();
        expect($('body').getText()).toContain('Password successfully changed.');
        browser.get('/');
      }
      changePassword('adminpw', 'new password');
      changePassword('new password', 'adminpw');
    });
};
