/* Common app functionality */
/**
 * this only runs client side so no point
 * in defining it server side too
 * use presence of HtmlService to detect Server side
 */
var App = this.HtmlService ? null :  (function startApp (app) {
  'use strict';
  
  
  // sets up all the app management divs.
  app.initialize = function () {
    
    app.showNotification = function (header, text, toast) {
      DomUtils.elem('notification-header').innerHTML=header;
      DomUtils.elem('notification-message').innerHTML =text;
      console.log ('app-notification', header , text , toast);
      if (toast) {
        DomUtils.applyClass ("notification-area", false, "notification-error-header");
        DomUtils.applyClass ("notification-area", true, "notification-toast-header");
        DomUtils.applyClass('notification-header',false, "notification-error-header");  
        DomUtils.applyClass('notification-message',false, "notification-error-message");  
        DomUtils.applyClass('notification-header',true, "notification-toast-header");  
        DomUtils.applyClass('notification-message',true, "notification-toast-message"); 
      }
      else {
        DomUtils.applyClass ("notification-area", false, "notification-toast-header");
        DomUtils.applyClass ("notification-area", true, "notification-error-header");
        DomUtils.applyClass('notification-header',false, "notification-toast-header");  
        DomUtils.applyClass('notification-message',false, "notification-toast-message");  
        DomUtils.applyClass('notification-header',true, "notification-error-header");  
        DomUtils.applyClass('notification-message',true, "notification-error-message"); 
      }
      DomUtils.hide ("notification-area", false);
 
    };
    
    app.hideNotification = function () {
      DomUtils.hide('notification-area',true);
    };
    
    app.toast = function (header,text) {
      app.showNotification (header,text,true);
      setTimeout (function () {
        app.hideNotification();
      },10000);
    }
    
    DomUtils.elem('notification-close').addEventListener("click", function () {
      DomUtils.hide('notification-area',true);
    },false);
    
  };
  
  return app;
})({});


