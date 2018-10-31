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


  // mini expbackoff to recover from 409 communicationerrors
  ns.MAX_ATTEMPTS = 3;
  ns.EXP_TIME = 667;
  
  function isRedoable (err) { 
    const es = err && err.toString();
    return es && ["NetworkError: "].some (function (d) {
      return es.slice (0,d.length) === d;
    });
  }
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

    // wrap this is a new promise as runner may go multiple times
    return new Promise (function (resolve, reject) {
      return runner();

      // this might be executed multiple times
      //if we get communication errors
      function runner (attempt) {
      
        // for first call
        attempt = attempt || 0;
        google.script.run
      
        .withFailureHandler (function(err) {
          console.log ('error' ,err);
          if (attempt < ns.MAX_ATTEMPTS  &&  isRedoable (err) ) {
            // retry because of communication errors - basic exp backoff
            const waitTime = Math.pow(2, attempt) * ns.EXP_TIME + (ns.EXP_TIME/2*Math.random());
            attempt++;
            return ns.loiter ()
              .then (function () {
                console.log ("Provoked exp backoff retry " + attempt + " after waiting for " + waitTime);
                return runner (attempt);
              });
          }
          else {
            reject (err);
          }
        })
      
        .withSuccessHandler (function(result) {
          resolve (result);
        })
      
        .exposeRun (namespace,method,runArgs); 
      };
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





