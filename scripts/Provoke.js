/**
* used to expose memebers of a namespace
* this runs Server side
* but i include it this file
* since it's called from Client side by Provoke.
* @param {string} namespace name
* @param {method} method name
*/
function exposeRun(namespace, method, argArray) {
  
  var global = this;
  var func = namespace ? global[namespace][method] : global[method];

  if (argArray && argArray.length) {
    return func.apply(this, argArray);
  } else {
    return func();
  }
}

/**
 * this only runs client side so no point
 * in defining it server side too
 * use presence of HtmlService to detect Server side
 */
var Provoke = this.HtmlService ? null :(function (ns) {

  /**
  * run something asynchronously
  * @param {string} namespace the namespace (null for global)
  * @param {string} method the method or function to call
  * @param {[...]} the args
  * @return {Promise} a promise
  */
  ns.run = function (namespace,method) {


    // the args to the server function
    var runArgs = Array.prototype.slice.call(arguments).slice(2);

    if (arguments.length<2) {
      throw new Error ('need at least a namespace and method');
    }

    // this will return a promise
    return new Promise(function ( resolve , reject ) {
      
      google.script.run
    
      .withFailureHandler (function(err) {
        reject (err);
      })
    
      .withSuccessHandler (function(result) {
        resolve (result);
      })
    
      .exposeRun (namespace,method,runArgs); 
    });
    
    
  };
  
  /*
  * settimeout as a promise
  * @param {number} ms number of ms to wait
  * @param {*} [tag] optional tag that will be returned when resolved
  */
  ns.loiter = function (ms, tag) {
    return new Promise(function(resolve, reject) {
      try {
        setTimeout(function() {
          resolve(tag);
        }, ms);
      } catch (err) {
        reject(err);
      }
    });
  };
  
  return ns;
  
})(Provoke || {});





