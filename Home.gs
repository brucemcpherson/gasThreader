
/**
 * this only runs client side so no point
 * in defining it server side too
 * use presence of HtmlService to detect Server side
 */
var Home = this.HtmlService ? null : (function (ns) {
  'use strict';

  // The initialize function must be run to activate elements
  ns.init = function () {


    
  };

  
  return ns;
  
})(Home || {});

