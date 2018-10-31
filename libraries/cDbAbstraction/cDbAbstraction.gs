/**
 * @fileOverview This is the dbabstraction data handler as described in http://ramblings.mcpher.com/Home/excelquirks/dbabstraction
 * @author <a href="mailto:bruce@mcpher.com">Bruce McPherson</a>
 */

/**
 * DbAbstraction
 * @constructor DbAbstraction
 * @param {object} driverLibrary reference to the library for the selected driver
 * @param {object} options - all the options as below
 *   @param {string}  tablename or silo
 *   @param {number} [expiry] cache expiry time in seconds
 *   @param {string} [dbid] any additional driver specific item to identify it
 *   @param {object} [driverob] any additional driver specific object
 *   @param {boolean} [randomsilo=false] generate a randome silo id
 *   @param {boolean} [optout=false] opt out of analytics
 *   @param {string} [peanut] some unique user id for analaytics
 *   @param {string} [accesstoken] an oauth2 access token
 *   @param {number} [disablecache=false] don't use cache at all with this one.
 *   @param {Cache} [specificcache] a cache object to use, otherwise a public/private one belonging to cacheservice will be used
 *   @param {string} [cachecommunity] a cache community id to restrict cache to particular groups
 *   @param {boolean} [private=true] whether to use a private cache (not relevant if specificcache is passed)
 *   @param {ENUM.SETTINGS.TRANSACTIONS} [transactions=ENABLED] whether to allow transactions
 *   @param {ENUM.SETTINGS.LOCKING} [locking=ENABLED] what kind of locking to do
 *   @param {number} waitafter the number of seconds to wait after a write operation. This should typically be zero, bur orchestrate has a non zero default
 */

"use strict";

function getLibraryInfo () {

  return { 
    info: {
      name:'cDbAbstraction',
      version:'2.2.4',
      key:'MHfCjPQlweartW45xYs6hFai_d-phDA33',
      description:'abstraction database handler',
      share:'https://script.google.com/d/1Ddsb4Y-QDUqcw9Fa-rJKM3EhG2caosS9Nhch7vnQWXP7qkaMmb1wjmTl/edit?usp=sharing'
    },
    dependencies:[
      cCacheHandler.getLibraryInfo(),
      cUseful.getLibraryInfo(),
      cFlatten.getLibraryInfo(),
      cUAMeasure.getLibraryInfo(),
      cNamedLock.getLibraryInfo()
    ]
  }; 
}

function DbAbstraction ( driverLibrary , options) {

  options = options || {};
        
  // legacy - translate object to old style arguments
  var tableName = options.siloid;
  var optExpiry = options.expiry;
  var driver,type;
  var optDriverSpecific = options.dbid;
  var optDriverOb = options.driverob;
  var optRandomSilo = options.randdomsilo;
  var optOptOut = options.optout;
  var optPeanut = options.peanut;
  var optAccessToken = options.accesstoken;
  var optDisableCache = options.disablecache;
  var optSpecificCache = options.specificcache;
  var self = this;
  
  var optOut = typeof optOptOut === typeof undefined ? true : optOptOut;
  var peanut = optPeanut || '';
  var accessToken = optAccessToken || '';
  var disableCache = optDisableCache || false;
  var cacheCommunity = options.cachecommunity;
  var currentLock_ = null;
  var private = options.private;
  
  /** 
   * give access to constants
   * @memberof DbAbstraction
   * @return {object} constant enums
   */
  this.getEnums = function (){
    return dhConstants;
  };
  
  var enums = self.getEnums();
  
  var locking_ = options.locking || enums.LOCKING.ENABLED;
  var transactions_ = options.transactions || enums.TRANSACTIONS.ENABLED;
  
  
  this.lockingState = function () {
    return locking_;
  };
  
  this.transactionsState = function () {
    return transactions_;
  };
 
  this.allDone = function () {
    return self.makeResults(enums.CODE.OK);
  }
  
 /** create the driver version
  * @return {string} the driver version
  */ 
  self.getVersion = function () {
    var v = getLibraryInfo().info;
    return v.name + ':' + v.version;
  };
  
  /**
   * recursive rateLimitExpBackoff()
   * @memberof DbAbstraction
   * @param {function} callBack some function to call that might return rate limit exception
   * @param {number} [sleepFor|enums.SETTINGS.SLEEPFOR] optional amount of time to sleep for on the first failure in missliseconds
   * @param {number} [maxAttempts|enums.SETTINGS.MAXATTEMPTS] optional maximum number of amounts to try
   * @param {number} [attempts=1] optional the attempt number of this instance - usually only used recursively and not user supplied
   * @return {*} results of the callback 
   */
  this.rateLimitExpBackoff = function ( callBack, sleepFor ,  maxAttempts, attempts  ) {
    var self = this;
    return cUseful.rateLimitExpBackoff(
        callBack, 
        sleepFor || self.getEnums().SETTINGS.SLEEPFOR, 
        maxAttempts || self.getEnums().SETTINGS.MAXATTEMPTS, 
        attempts,
        true
      );
  };
  
  /**
   * forces a wait after a write for options.waitafter milliseconds
   * the only driver i think this is needed for is orchestrate which has latency in its network 
   * you only need this if you want to ensure that the change orchestrate has reported is committed is propogated through its infrastructure
   * otherwise set it to 0 - the default
   * @param {function} func what to do before waiting
   * @param {*} return whater func() returns
   */
  this.waitAfter = function (func) {
  
    // do the thing
    var r = func();
    
    // wiat a bit if necessary (only known driver this has any effect is orchestrate
    if (options.waitafter) {
      Utilities.sleep(options.waitafter);
    }
    else if (driver.WAITAFTER) {
      Utilities.sleep (driver.WAITAFTER);
    }
    
    // the function result
    return r;
  }
  /** 
  * return unique string
  * @memberof DbAbstraction
  * @return {string} a unique string
  */
  this.generateUniqueString = function () {
    return cUseful.generateUniqueString(self.getEnums().SETTINGS.ULENGTH);
  };
  /**
   * append an aray to another
   */
  this.arrayAppend = function (a,b) {
    return cUseful.arrayAppend(a,b);
  };

  this.unFlatten = function (obs) {
    if (!obs) return null;
    
    var flat = new cFlatten.Flattener().setKeepDates (driver.keepDates), result;
    if (!Array.isArray(obs)) {
      result =  flat.unFlatten(obs);
    }
    else {
      result = obs.map (function(d) {
        return flat.unFlatten(d);
      });
    }
    
    return result;
  };

  /**
   * flatten an array of objects/a single object
   * @param {Array.Object|Object} obs an array of/single unflattened objects
   * @param {boolean} optConstraints whether there might be constraints to preserve
   * @return {Array.Object|Object} an array of/single flattened objects
   */
  this.flatten = function (obs,optConstraints) {
    // flatten an object
    if (!obs) return null;
    if (Array.isArray(obs)) {
      return obs.map(function(d) {
        return new cFlatten.Flattener(optConstraints ? self.getEnums().SETTINGS.CONSTRAINT : null).setKeepDates (driver.keepDates).flatten(d);
      });
    }
    else {
      return new cFlatten.Flattener(optConstraints ? self.getEnums().SETTINGS.CONSTRAINT : null).setKeepDates (driver.keepDates).flatten(obs);
    }
  };
  
/**
 * DbAbstraction.escapeQuotes()
 * @memberof DbAbstraction
 * @param {string} str string to be escaped
 * @return {string} escaped string
 */
  this.escapeQuotes = function( str ) {
    return cUseful.escapeQuotes(str);
  };

  /** 
   * check if an item is an object
   * @memberof DbAbstraction
   * @param {*} obj an item to be tested
   * @return {boolean} whether its an object
   */
  this.isObject = function (obj) {
      return cUseful.isObject(obj);
  };

  this.clone = function (o) {
    return cUseful.clone(o);
  };
  
  /**
   * get rid of fields that are specific to a driver
   * @param {Array.string} drop the name of the driverfields to drop
   * @param {string} keyName the keyName to extract -if you want it dropped too add to drop
   * @param {Array.object} obs the objects to drop them from
   * @return {object} {[obs],[keys]}
   */
  this.dropFields = function ( drop , keyName , obs) {
    
    // just in case its not an array

    if (!Array.isArray(drop)) drop = drop ? [drop] : [];
    if (!Array.isArray(obs)) obs = obs ? [obs] : [] ;
    
    // drop all the unwanted fields
    return obs.reduce(function (p,c) {
     
      // split up the object into keys and data, and drop any unwanted field.
      var x =  Object.keys(c).reduce(function (cp,cc) {
        // we keep it
        if(drop.indexOf(cc) === -1) {
          cp.ob[cc] = c[cc];
        }
        //transfer the key
        if (cc === keyName) {
          cp.key = c[cc];
        }
        return cp;
      },{ob:{},key:undefined});
      
      // add to the pile
      p.obs.push(x.ob);
      p.keys.push(x.key);
      
      return p;
    },{obs:[],keys:[]});
  };
  
  this.checksum = function checksum(o) {
    // just some random start number
    var c = 23;
    var s =  (self.isObject(o) || Array.isArray(o)) ? JSON.stringify(o) : o.toString();
    for (var i = 0; i < s.length; i++) {
      c += (s.charCodeAt(i) * (i + 1));
    }

    return c;
  }

  var randomSilo = optRandomSilo || false;
  var siloId = randomSilo ? self.generateUniqueString() + tableName : tableName;
  var driverSpecific = optDriverSpecific;
  var driverOb = optDriverOb || null;
  var expiry = optExpiry || enums.SETTINGS.CACHEEXPIRY;

  
  this.getLockName = function (optType) {
    return {type:optType || type,key:driverSpecific,id:siloId};
  };
  
  this.lock = function (optType) {
      return new cNamedLock.NamedLock(enums.SETTINGS.PROTECTEXPIRY).setKey(self.getLockName(optType)); 
  };
  
 /** 
   * select and open handler for the backend driver
   * @memberof DbAbstraction
   * @return {object} driver for the database - normally used only inside this class
   */
   
  this.setDriver = function () {
      try {
        return driverLibrary.createDriver(self,siloId,driverSpecific,driverOb, accessToken,options);
      }
      catch(err) {
        throw err + '-an unknown database type';
      }
  };
  
  var driver = self.setDriver();
  var type = driver.getType();
  var cacheSilo = 's'+siloId+'t'+type+'d'+driverSpecific;
  var cacheHandler = new cCacheHandler.CacheHandler(expiry,cacheSilo,private,disableCache,optSpecificCache,cacheCommunity);
  var cacheVoid = new cCacheHandler.CacheHandler(expiry*2,cacheSilo,false,false,null,'void');
  var transactionId_;
  
  /** analytics measurement
   */
   
  var ua,uap;
  if (!optOut) {
    uap = PropertiesService.getScriptProperties().getProperty(enums.SETTINGS.UAKEY);
    if (uap) { 
      var uaProperties = JSON.parse(uap);
      ua = new cUAMeasure.UAMeasure (uaProperties.uaCode, uaProperties.property, peanut, optOut , self.getVersion());
    }
  }
  

  
  /** 
   * return the back end handler
   * @memberof DbAbstraction
   * @return {object} driver for the database 
   */
   
  this.getDriver = function () {
    return driver;
  };
  
  /** 
   * DbAbstraction.getTableName()
   * @memberof DbAbstraction
   * @return {string} table name or silo
   */
  this.getTableName = function () {
    return driver.getTableName();
  };

  
  this.getDBName = function () {
    for (var k in enums.DB) {
      if (type === enums.DB[k]) {
        return k;
      }  
    }
    throw ('DB-' + type + ' error code unknown:programming error-');
  };
  
  this.getTransactionId = function () {
    return transactionId_;
  };
  
  
  this.clearTransaction = function () {
    transactionId_ = null;
  };
  
  this.setTransaction = function () {
    transactionId_ = cUseful.generateUniqueString(3);
    return self.getTransactionId();
  };
  
  
  this.rollBack = function () {
    // doesnt do anything yet
    return enums.CODE.TRANSACTION_ROLLBACK_FAILED;
  }


  
  /** 
   * organize given constraints
   * @memberof DbAbstraction
   * @param {object|object[]} constraints constratints to be organized
   * @return {object} an object containing all the constraints
   */

  this.constraints = function (constraints) {
    if (!Array.isArray(constraints)) constraints = [constraints];
    if (!constraints.length) return null;
    
    if (!Array.isArray(constraints[0])) constraints = [constraints];
    
    var e={};
    e[enums.SETTINGS.CONSTRAINT] = constraints.map(function(c){ 
      if (!c[0]) throw ('unknown constraint:' + JSON.stringify(c));
      return {constraint:c[0],value:c[1]};
    });
    return e;
  };
  
  this.getErrorCode = function (hCode,messageText) {
    var code;
    for (var k in enums.CODE) {
      if (hCode === enums.CODE[k]) {
        code =k;
        break;
      }  
    }
    if (typeof code==='undefined' ) { 
      throw ('code' + hCode + ' error code unknown:programming error-' + (messageText || ''));
    }
    return code;
  };
  
  this.getErrorText = function(hCode) {
    return enums.ERROR[self.getErrorCode(hCode)];
  }
  /**
   * DbAbstraction.makeResults()
   * @memberof DbAbstraction
   * @param {dhConstants.CODE} handleCode the error code 0 = good, -ve = bad, +ve = warning
   * @param {string} messageText any additional error text to add to the handleError
   * @param {object} result the result code returned from the driver
   * @return {object} results 
   */
   
  this.makeResults = function (handleCode,messageText,result,driverIds,handleKeys) {
    var handleError;

    var code = self.getErrorCode(handleCode,messageText);
    
    handleError = (messageText ? " (" + messageText + ") " : '') + enums.ERROR[code];
    
    // check that the results are valid
    if (handleCode === enums.CODE.OK && result && result.error) {
      handleError += "(" + enums.ERROR.RESULT + ")";
      handleCode = enums.CODE.RESULT;
    }
    if (handleError) {
      handleError = '(DbAbstraction says:'+code+') '+handleError;
    }
    
    var r = (result === null || typeof result === 'undefined') ? [] : result;

    var ret = {handleCode:handleCode, handleError:handleError,data:r,
            handleVersion:self.getVersion()};
    if (driver) {
      ret.driverVersion = driver.getVersion();
      ret.table = driver.getTableName();
      ret.dbId = driver.getDbId();
      ret.keyProperty = driver.getKeyProperty ? driver.getKeyProperty() : 'key';
    }
    else {
      ret.driverVersion = 'unknown driver version';
    }
    if (handleKeys) {
      ret.handleKeys = handleKeys;
    }
    
    if (driverIds) {
      ret.driverIds = driverIds;
    }

    return ret;
  };

  this.uaPage = function (action) {
    return 'db'+ '_' + action +'_' +  driver.getVersion();
  };
  

  
 /**
   * DbAbstraction.remove()
   * @memberof DbAbstraction
   * @param {object} [queryOb] some query object 
   * @param {object} [queryParams] additional query parameters (if available)
   * @param {boolean} [noCache=0] whether to suppress cache
   * @return {object} results from selected handler
   */
  
  this.remove = function (queryOb,queryParams) {

    
    return self.uaWrap ('remove', function () {
      self.voidCache();
      return driver.remove(queryOb,queryParams);
    });
   
  };
  
  this.uaWrap = function (what, func) {
    
    if (ua) {
      ua.postAppView(self.uaPage (what));
    }
    var result = func();
    if (ua) {
      ua.postAppKill();
    }
    return result;
  };
   /**
   * DbAbstraction.removeByIds()
   * @memberof DbAbstraction
   * @param {Array.string} ids list of handleKey ids to remove
   * @return {object} results from selected handler
   */
  
  this.removeByIds = function (ids) {
  
    if (!ids) { 
      return self.makeResults(enums.CODE.NO_ACTION,'list of ids not present in removeByIds');
    }
    
    return self.uaWrap ('removeByIds', function () {
      if(!Array.isArray(ids)) ids = [ids];
      self.voidCache();
      return driver.removeByIds(ids);
    });
    
  };
  /**
   * DbAbstraction.save()
   * @memberof DbAbstraction
   * @param {object[]} obs array of objects to write
   * @return {object} results from selected handler
   */
  this.save = function (obs) {
    
    return self.uaWrap ('save', function () {
      self.voidCache();
      var obArray = Array.isArray(obs) ? obs : [obs];
      return driver.save(obArray);
    });
    
  };

  /**
   * should be called when a databse update is done
   * any cache entries with an earlier timestamp are not valid
   * return void
   */
  this.voidCache = function () {
    cacheVoid.putCache (self.getCob ([], 'v'),"v");
  };
  
  /**
   * call with a cache object
   * if its timestamp is earlier that the last update for this silo it will return null
   * @param {object} cob 
   * @return {object | null } the arg or null if invalid
   */
  this.usableCache = function (cob) {
    
    if (cob && cob.hasOwnProperty('cacheProperties')) {
      // need to get the cob associated with the silo
      var siloCob = cacheVoid.getCache ("v");
      return siloCob && siloCob.cacheProperties.timeStamp >= cob.cacheProperties.timeStamp ? null : cob;
    }
    else {
      return cob;
    }
  };
  /**
   * create a timestamped cache object
   * @param {obect} data the data
   * @param {string} operation the operation
   * @return {object} the cob
   */
  this.getCob = function (data, operation) {
    return { 
      data:data,
      cacheProperties: {
        timeStamp:new Date().getTime(), 
        operation:operation,
        siloKey:cacheSilo
      }
    };
  }
  
  this.cobMessage = function (cob) {
    return cob && cob.hasOwnProperty('cacheProperties') ? 
      JSON.stringify(cob.cacheProperties) + " (was written at " +  
      Utilities.formatDate(new Date(cob.cacheProperties.timeStamp),"GMT","yyyy-MM-dd HH:mm:ss.SSS") + ") " : "";
  };
  
  this.cobbery = function (queryOb, queryParams, noCache , operation) {
    var cob,cached;
    
    //we dont use cache if in a transaction
    if (!self.getTransactionId()) {
      // if cache is noy disabled
      if (!noCache && ! disableCache) {
        // make sure that cache is still viable and get dataif any
        cob = self.usableCache(cacheHandler.getCache(queryOb,queryParams,operation));
        cached = cob && cob.hasOwnProperty('cacheProperties') ? cob.data : cob;
      }
    }
    
    // will return a results package or null
    return cob ? self.makeResults(enums.CODE.CACHE, self.cobMessage(cob), cached) : null;
    
  };
  
  /**
   * DbAbstraction.count()
   * @memberof DbAbstraction
   * @param {object} [queryOb] some query object 
   * @param {object} [queryParams] additional query parameters (if available)
   * @param {boolean} [noCache=0] whether to suppress cache
   * @return {object} results from selected handler
   */
  this.count = function (queryOb,queryParams,noCache) {
    
    return self.uaWrap ('count', function () {
        var cob = self.cobbery (queryOb,queryParams,noCache,"c");
        
        if (cob) {
          return cob;
        }
        else {
          var result = driver.count(queryOb,queryParams,noCache);
          if (result.handleCode <0) {
              self.voidCache();
          }
          else {
            cacheHandler.putCache (self.getCob(result.data,"c"),queryOb,queryParams,"c");
          }
          return result;
        }
    });

    
  };
  
 /**
   * DbAbstraction.query()
   * @memberof DbAbstraction
   * @param {object} [queryOb] some query object 
   * @param {object} [queryParams] additional query parameters (if available)
   * @param {boolean} [noCache=0] whether to suppress cache
   * @param {boolean} [optKeepIds=false] whether or not to keep driver specifc ids in the results
   * @return {object} results from selected handler
   */
  this.query = function (queryOb,queryParams,noCache,optKeepIds) {
    var cached,result;

    var keepIds = (typeof optKeepIds === 'undefined' ?  enums.SETTINGS.KEEPIDS : optKeepIds);
    var cob;
    
    // we dont do caching if we need the ids
    if (!optKeepIds) { 
      cob = self.cobbery (queryOb,queryParams,noCache,"q");
    }
    
    if (cob) {
      return cob;
    }
    else {
      return self.uaWrap ('query', function () {
        // if queryOb is an array then we are doing an OR
        var doingOR = Array.isArray(queryOb);
        if (doingOR) {
          if (!Array.isArray(queryParams)) {
            queryParams = [queryParams];
          }
          var datas = [],keys =[],dIds =[];
          code = enums.CODE.OK;
          err = '';
          queryOb.forEach (function (q,i) {
         
            var t = driver.query(q,queryParams.length === 1 ? queryParams[0] : queryParams[i],true);
            if (t.handleCode < 0) {
              code = t.handleCode;
              err = t.handleError;
            } 
            else {
              // concatenate results
              t.data.forEach (function (d,j) {
                if( !keys.some(function(k) { 
  
                    return k === t.handleKeys[j]; 
                  })) {
                    keys.push(t.handleKeys[j]);
                    dIds.push(t.driverIds[j]);
                    datas.push(d);
                  }
              });
              
            }
          });
  
          result = self.makeResults(code,err,datas,keepIds ? dIds :null,keepIds ? keys:null);
  
        }
        else {
          
          result = driver.query(queryOb,queryParams,keepIds);
        }
        if (result.handleCode <0) {
            self.voidCache();
        }
        else {
          cacheHandler.putCache (self.getCob(result.data,"q"),queryOb,queryParams,"q");
        }
        return result;

      });
    }
      
    
  };
   /**
   * DbAbstraction.get()
   * @memberof DbAbstraction
   * @param {string} key some unqiue key as returned by handleKeys
   * @param {boolean} [noCache=0] whether to suppress cache
   * @param {boolean} [keepIds=false] whether or not to keep driver specifc ids in the results
   * @return {object} results from selected handler
   */
  
  this.get = function (key,noCache,optKeepIds) {

    return self.uaWrap ('get', function () {
      var cached;
      var keepIds = (typeof optKeepIds === 'undefined' ?  enums.SETTINGS.KEEPIDS : optKeepIds);
      var cString = "g"+keepIds;

      var cob;
      if(!optKeepIds) { 
        cob = self.cobbery (key,undefined,noCache,cString);
      }
      if (cob) {
        return cob;
      }
      else {

        var result = driver.get(key,keepIds);

        if (result.handleCode <0) {
            self.voidCache();
        }
        else {
          cacheHandler.putCache (self.getCob(result.data,cString),key,undefined,cString);
        }
        
        return result;
      }
    });

  };
 /**
   * DbAbstraction.update()
   * @memberof DbAbstraction
   * @param {string} key some unqiue key as returned by handleKeys
   * @param {object}  ob what to update it to
   * @return {object} results from selected handler
   */
  
  this.update = function (key,ob) {
    
    return self.uaWrap ('update', function () {
        self.voidCache();
        return driver.update(key,ob);
    });
    
  };
  /**
   * DbAbstraction.getDriveHandle()
   * @memberof DbAbstraction
   * @return {object} the driver handle
   */
  this.getDriveHandle = function () {
    return  driver.getDriveHandle() ;
  };
  
  /**
   * DbAbstraction.isHappy()
   * @memberof DbAbstraction
   * @return {boolean} whether the driver is ready to use
   */
  this.isHappy = function () {
    return (driver && self.getDriveHandle() && driver.getTableName() && type ) ? true : false;
  };
  
  // utility functions for use by driver
  
  
  this.makeUseParams = function(paramsData) {
    // set a more convenient params object
    return paramsData.reduce (function (p,c) {
      p[c.param] = c;
      return p;
    },{skip:{skip:0},limit:{limit:0},sort:null});
  };
  /**
   * DbAbstraction.getQueryParams()
   * @memberof DbAbstraction
   * @param {object} [queryParams] additional query parameters (if available)
   * @return {object} results the parameters sorted out
   */
  this.getQueryParams = function(queryParams) {
    var result =[], handleError='', handleCode=enums.CODE.OK, order =['sort','skip','limit'];
    
    if (queryParams) {
       
       // need to ensure we do sort before limit
        result = Object.keys(queryParams).sort (function (a,b) {
          var ka = order.indexOf(a);
          var kb = order.indexOf(b);
          // deal with unknown parameters
          if (kb === -1) return 1;
          if (ka === -1) return -1;
          // sort according to order in order array
          return  ka < kb ? -1 : (ka === kb ? 0 : 1);
        })
        .map (function(q) {

          if (q==='sort') {
            var sortDescending = (queryParams[q].slice(0,1) === '-');
            var sortKey = sortDescending ? queryParams[q].slice(1) : queryParams[q];
            return {param:q,sortKey:sortKey,sortDescending:sortDescending,value:queryParams[q]};
          }
          else  { 
            var v = parseInt(queryParams[q],10);
            if (q==='limit' || q==='skip') {
              if (!isNaN(v)) {
                return q === 'limit' ? {param:q,limit:v,value:queryParams[q]} : {param:q,skip:v,value:queryParams[q]};
              } 
              else {
                handleError = 'Invalid vaue for ' + q + ':' + queryParams[q];
                handleCode = enums.CODE.PROPERTY;
                return null;
              }
            }
            else {
            // this could be some custom parameter for the driver i cant validate
              return {param:q,value:queryParams[q]};
            }
          }
        });

    }
    
    return self.makeResults(handleCode,handleError,result);
  };
 
  /**
   * DbAbstraction.makeQuote()
   * @memberof DbAbstraction
   * @param {*} item quote a string if its a string
   * @param {string} [optForce=false] if this is supposed to be a string, convert it to as tring and quote it
   * @param {string} [optTheQuote='] the kind of quote to use
   * @return {object} escaped string
   */
  this.makeQuote = function (item,optForce,optTheQuote) {
    // add quotes if necessary
    var theQuote = optTheQuote || "'";
    var fType = optForce ? optForce.toUpperCase() : '';
    Logger.log('optforce ' + fType);
    if ( ( fType !== "NUMBER") || (!fType && (typeof item === "string" || cUseful.isDateObject(item))) ) {
      return theQuote + item.toString().replace(theQuote, "\\" + theQuote ) + theQuote;
    }
    else {
      return isNaN(item) && optForce && optForce.toUpperCase() === "NUMBER" ? theQuote + theQuote : item;
    }
  
  };

  /**
   * DbAbstraction.cleanPropertyName()
   * @memberof DbAbstraction
   * @param {string} name tidy of this name to contain good property names
   * @return {object} cleaner string
   */
  this.cleanPropertyName = function (name) {
    var a,b,r=/[^a-zA-Z_#@0-9\.]/;
    if (name) {
      a = name.slice(0,1);
      if (/[^a-zA-Z_]/.test(a)) {
        name = "_" + name;
      }
      name = name.replace ( r , "_");
      if (name.length > enums.SETTINGS.MAXPROPERTYNAME) name = name.slice(0,enums.SETTINGS.MAXPROPERTYNAME);
    }
    
    return name;
  };
  
  /**
   * DbAbstraction.renameProperty()
   * @memberof DbAbstraction
   * @param {object} ob  object containing the property to be renamed
   * @param {string} fromProp the name of property to be renamed  
   * @param {string} toProp what to rename the property as
   * @return {object} the object
   */
   
  this.renameProperty = function(ob,fromProp, toProp) {
    ob[toProp] = ob[fromProp];
    delete ob[fromProp];
    return ob;
  };
  /**
   * DbAbstraction.cleanPropertyNames()
   * @memberof DbAbstraction
   * @param {object} ob  object containing the properties to be checked and cleaned up
   * @return {object} the object
   */
  this.cleanPropertyNames = function (ob) {
  
    if (ob) {
      var ks = [];
      // because the indices will get touched...
      for (var k in ob) {
        ks.push(k);
      }
      
      ks.forEach(function(k) {
        
        // recurse for children
        if (typeof ob[k] === 'object' ) {
          self.cleanPropertyNames (ob[k]);
        }
        
        if (!Array.isArray(ob)) {
          var a = self.cleanPropertyName(k);
          var idx = 0,t='';
          
          // avoid duplicate property names
          if (a !== k) {
            while (ob.hasOwnProperty(a+t)) {
              t = a + "_" + idx;
            }
            self.renameProperty (ob , k , a+t);
          }
        }
  
      });
    }
    return ob;
  };
  
  /**
   * DbAbstraction.processParams()
   * @memberof DbAbstraction
   * @param {object} [queryParams] apply these parameters
   * @param {object} inputData to this data  
   * @return {object} a results object containing the status and the modified data
   */  
   
  this.processParams = function (queryParams,inputData) {
    var handleError='',handleCode=enums.CODE.OK,hk =[];

     // need to remember the index
    var sData = inputData.map (function (d,i) {
      return {d:d,index:i};
    });
    var params = self.getQueryParams(queryParams);

    if (params.handleCode === enums.CODE.OK) {

      // we have an array of good query params
      
      if(params.data.length) {
        params.data.forEach( function (e) {
          if(e.param === 'sort') {
            sData.sort (function (a,b) {
              var ad = a.d.data ? a.d.data : a.d;
              var bd = b.d.data ? b.d.data : b.d;
              var as = ad[e.sortKey],bs = bd[e.sortKey];
              return (as < bs ? -1 : ( as===bs ? 0 : 1)) * (e.sortDescending ? -1 : 1);
            });
          }
          
          else if (e.param === 'limit') {
            sData = sData.slice(0,e.limit);
          }
          
          else if (e.param ==='skip') {
            sData = sData.slice (e.skip);

          }
          else {
            handleError = e.param;
            handleCode = enums.CODE.PARAMNOTIMPLEMENTED;
          }
        });
      }
    }
    else {
      handleError = params.handleError;
      handleCode = params.handleCode;
    }
    
    return self.makeResults(handleCode,handleError,sData.map(function(d) { return d.d; }),undefined,sData.map(function(d) { return d.index; }));
    
  };

  /**
   * DbAbstraction.processFilters() - can be used to post process data if the driver is unable to handle constraints or filters
   * @memberof DbAbstraction
   * @param {object} [queryOb] apply these constraints and query
   * @param {object} inputData to this data  
   * @return {object} a results object containing the status and the modified data
   */  
   
  this.processFilters = function (queryOb,inputData) {
    var handleError='',handleCode=enums.CODE.OK;
    var sData = inputData.map (function (d,i) {
      return {d:d,index:i};
    });
    
    if (queryOb) {
      var fob = new cFlatten.Flattener(enums.SETTINGS.CONSTRAINT).setKeepDates (driver.keepDates).flatten(queryOb);
      var f = new cFlatten.Flattener().setKeepDates (driver.keepDates);
      sData = sData.filter (function (row,i) {
        var rd = f.flatten(row.d.data ? row.d.data : row.d);
        return Object.keys(fob).every(function(k) {
          return self.constraintFilter(rd,fob,k);
        });

      });
    }

    return self.makeResults(handleCode,handleError,sData.map(function(d) { return d.d; }),undefined,sData.map(function(d) { return d.index; }));
  };
  
 
  
  /**
   * DbAbstraction.processFilters() - can be used to post process data if the driver is unable to handle constraints 
   * @memberof DbAbstraction
   * @param {object} rd flattened data row
   * @param {object} fob a flattened object
   * @param {object} k the index of the query item to process
   * @return {boolean} whether this row should be included in the result
   */ 
    
  this.constraintFilter = function (rd,qob,k) {
    if (qob[k].hasOwnProperty(enums.SETTINGS.CONSTRAINT)) {
      return qob[k][enums.SETTINGS.CONSTRAINT].every ( function (c) {
        var good=  ( c.constraint === enums.CONSTRAINTS.LT && rd[k] < c.value ) ||
                 ( c.constraint === enums.CONSTRAINTS.LTE && rd[k] <= c.value ) ||
                 ( c.constraint === enums.CONSTRAINTS.GT && rd[k] > c.value ) ||
                 ( c.constraint === enums.CONSTRAINTS.GTE && rd[k] >= c.value ) ||
                 ( c.constraint === enums.CONSTRAINTS.NE && rd[k] !== c.value ) ||
                 ( c.constraint === enums.CONSTRAINTS.EQ && rd[k] === c.value ) ||
                 ( c.constraint === enums.CONSTRAINTS.IN && c.value.indexOf(rd[k]) >= 0) ||
                 ( c.constraint === enums.CONSTRAINTS.NIN && c.value.indexOf(rd[k]) < 0 ) ;
        return good;
      });
    }
    else if (rd.hasOwnProperty(k)) {
      return rd[k] === qob[k];
    }
    else {
      handleError =  k;
      handleCode = enums.CODE.PROPERTY;
      return false;
    }
  };
  
    
 /**
  * checks that the transaction matches the one stored
  * @param {string} id transaction id
  * @return {boolean} whether id matches
  */ 
  self.isTransaction = function (id) {
    return driver.getTransactionBox && driver.getTransactionBox() && driver.getTransactionBox().id === id ;
  };
  
  self.inTransaction = function () {
    return self.isTransaction( self.getTransactionId());
  };
  
  /**
   * transaction wrapper
   * @param {function} transactionFunction things that need to happen inside a transaction
   * @param {object} options any options you want to pass on to your transaction function
   * @return {object} a normal result package
   */
  this.transaction = function (transactionFunction , options) {
    
    // get id of current transaction if any
    var id = self.getTransactionId();
    
    // if there was already one under way that's a bad thing
    if (id) { 
      // report a transaction within a transaction as an error
      var results= makeResults (enums.CODE.TRANSACTION_FAILURE);
      results.transaction = {
        id: id,
        code: enums.CODE.OK,
        error:enums.ERROR.TRANSACTION_FAILURE
      };
      results.transaction = transaction;
      return results;
    }

    
    // set up new transaction
    var transaction = {
      id: self.setTransaction(),
      code: enums.CODE.OK,
      error:''
    };

    // the rules
    // if a driver is transaction aware
    //   it knows about locking regime of a transaction
    //   the entire transaction will be locked
    //   the items of the transaction are forbidden to do any locking
    // further, a transaction capable driver will be able to do rollback etc.
    // for non-aware drivers, a transaction is a non event - nothing happens, the parts of it are executed normally
    // if transactions are disabled, then all drivers are treated as non-transaction aware
    var transactionAware = driver.transactionAware && self.transactionsState() === enums.TRANSACTIONS.ENABLED;
    var transactionCapable = transactionAware && driver.transactionCapable;
    
    // if locking is disabled then no locking is done on the transaction
    var lockingEnabled = self.lockingState() !== enums.LOCKING.DISABLED && !driver.lockingBypass;
    var transactionLockBypass = !transactionAware || !lockingEnabled;    
    
    // enclose the whole transaction 
    var result = doGuts_ ( transactionLockBypass ,  "transaction:"+transaction.id , function (bypass) {
      
      
      // only applies to drivers that are able to do transactions
      if (transactionCapable) {

        // let the driver know it's doing a transaction
        driver.beginTransaction(transaction.id);
        
        // do the work
        try {
          
          // get the current state data, and execute the transaction contents
          driver.transactionData();
          var r = transactionFunction (self , options);
          
          if (r.handleCode < 0 ) {
            localRollBack();
          }
          else {
            // commit the transaction
            if (driver.getTransactionBox().dirty) {
              self.voidCache();
            }
            var r = driver.commitTransaction(transaction.id);
            transaction.code = r.handleCode;
            transaction.error = r.handleError;
          }
        }
        catch (err) {
          localRollBack();
          var r = self.makeResults(enums.CODE.TRANSACTION_FAILURE, err);
        }

        return r;
        
      }
      else {
      
        // the function is executed normally.
        // the driver should detect whether it is part of a transaction and act accordingly
       
        transaction.code = enums.CODE.TRANSACTION_INCAPABLE;
        transaction.error = self.getErrorText(transaction.code);
        
        // do the thing
        return transactionFunction (self , options); 
      
      }
    
    });
    
    // mark transaction as over
    result.transaction = transaction;
    self.clearTransaction();
    return result;
    
    function localRollBack() {
      var r = driver.rollbackTransaction(transaction.id);
      transaction.code = r.handleCode < 0 ? enums.CODE.TRANSACTION_ROLLBACK_FAILED :  enums.CODE.TRANSACTION_ROLLBACK;
      transaction.error = self.getErrorText(transaction.code);
      self.voidCache();
    }
    
  };
  
  self.getCurrentLock = function () {
    return currentLock_;
  };
    
  
 /**
  * convenience function to apply  locking on a write type operation
  * @param {string} what comment for locking debuging
  * @param {function} func what to wrap if not in transaction
  * @param {function} transactionFunc to wrap
  * @return {object} whatever func() returns
  */
  self.readGuts = function (what, func, transactionFunc) {
    return doGuts_ (self.lockingState() !== enums.LOCKING.AGGRESSIVE  , 'aggressive ' + what , func, transactionFunc) ;
  };
 /**
  * convenience function to apply  locking on a write type operation
  * @param {string} what comment for locking debuging
  * @param {function} func what to wrap if not in transaction
  * @param {function} transactionFunc to wrap
  * @return {object} whatever func() returns
  */
  self.writeGuts = function (what, func, transactionFunc) {
    return self.waitAfter( function () {
      return doGuts_ (self.lockingState() === enums.LOCKING.DISABLED   ,  what , func , transactionFunc) ;
    });
  };
  
  function doGuts_ (bypass , what , func, transactionFunc ) {

    if (self.getCurrentLock()) {
      return transactionFunc ? transactionFunc(bypass) : func(bypass);
    }
    else {
      if (bypass) {
        // been told not to bother
        return func(bypass);
      }
      else {
          // need to get a lock and execute the function
          return self.lock().protect (what, function (lock) { 

            // save the allocated lock to avoid  deadlocking inside the lamda
            currentLock_ = lock;
            
            // run the function
            var result  = func(bypass) ;
            
            // all is good lock can be forgotten
            currentLock_ = null;
            return result;
            
          }).result;

      }
    }
  } 
  

   
  return self;
  
};


  