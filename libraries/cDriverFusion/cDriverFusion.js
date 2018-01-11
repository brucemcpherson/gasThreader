
/** wrapper
 */
function createDriver (handler,siloId,driverSpecific,driverOb, accessToken) {
    return new DriverFusion(handler,siloId,driverSpecific,driverOb, accessToken);
}
function getLibraryInfo () {
  return {
    info: {
      name:'cDriverFusion',
      version:'2.2.0',
      key:'MyhWrchJeGiOowTfrMNidiSz3TLx7pV4j',
      description:'Fusion driver for dbabstraction',
      share:'https://script.google.com/d/1wPX-hMhaX_vk_3cAlZ4CUJ6GeNOsm2VrQpUqG4QU3GWeM45AVMiOU0OD/edit?usp=sharing'
    },
    dependencies:[
    ]
  }; 
}
/**
 * DriverFusion
 * @param {cDataHandler} handler the datahandler thats calling me
 * @param {string} tableId this is the fusion table key
 * @param {string} title if tableId is blank, then a table of this name will be created
 * @param {object} fusionOb a fusion ob if required  ( { restAPIKey:"your fusion developer key"} )
 * @return {DriverFusion} self
 */
 
var DriverFusion = function (handler,tableId,title,fusionOb) {
  var siloId = tableId;
  var self = this;
  var parentHandler = handler;
  parentHandler.flatten
  var enums = parentHandler.getEnums(); 
  // im not able to do transactions
  self.transactionCapable = false;
  
  // i  need transaction locking
  self.lockingBypass = false;
  
  // i am aware of transactions and know about the locking i should do
  self.transactionAware = true;

  // i want to keep dates
  self.keepDates = true; 
  
  self.getType = function () {
    return enums.DB.FUSION;
  };

  var keyOb = fusionOb;
  // I try to do as many inserts in one go as possible to minimize rate limits
  // however there are quotas - this parameter is used to try to stay within them
  // bigger - less transactions, but more chance of failure (it fails server error and isnt caught with try/catch)
  var MAXCHUNKSIZE = 1000;
  var MAXINSERTS = 50;
  var DUMMYCOLUMN = '_dummy_';
  var handleError, handleCode;
  var tableTitle = title;
  
  self.getDbId = function () {
    return tableTitle;
  };
  
  self.createTable = function () {
  
    try {
    
      return  FusionTables.Table.insert({
        "name": title,
        "columns": [ {
          "name": DUMMYCOLUMN,
          "type": 'STRING',
          }
        ],
        "isExportable": true
      });
      
     }
     catch (err) {
       throw (err);
     }
  }
  
  if (tableTitle && !siloId) {
    siloId = self.createTable ().tableId;
  }
  var handle = self;
  
  self.getDriveHandle =  function () {
    return handle;
  };

 /** create the driver version
  * @return {string} the driver version
  */ 
  self.getVersion = function () {
    var v = getLibraryInfo().info;
    return v.name + ':' + v.version;
  };
  
  /**
   * DriverFusion.getTableName()
   * @return {string} table name or silo
   */
  self.getTableName = function () {
    return siloId;
  };

  function getConstraintName_ (constraint) {
 
    return constraint ? 
      Object.keys(enums.CONSTRAINTS).reduce (function(p,c) { 
        return enums.CONSTRAINTS[c] === constraint ? enums.FUSION_CONSTRAINTS[c] : p;
      },'') 
    : null;
  }
    
  function makeQuery (qob,qop) {
   var result =null,q='';
   handleError='', handleCode=enums.CODE.OK,fs=[];
   var queryOb = parentHandler.flatten(qob,true);
   var queryParams = parentHandler.flatten(qop);
   
    try {
      if (queryOb) {
        // make the query
        q = ''; 
        fs = Object.keys(queryOb).reduce(function (p,c) {

          if (parentHandler.isObject (queryOb[c])) {
            if (queryOb[c].hasOwnProperty (enums.SETTINGS.CONSTRAINT)) {
              queryOb[c][enums.SETTINGS.CONSTRAINT].forEach(function(d) {
                var op = getConstraintName_ (d.constraint);
                
                if (op) {
                  p.push ({name:c, value: d.value , op: op});
                }
              });
            }
            else {
              handleError = JSON.stringify(queryOb[c]);
              handleCode = enums.CODE.PROPERTY;
            }
          }
          else {
            p.push ({name:c, value:queryOb[c], op: getConstraintName_(enums.CONSTRAINTS.EQ)});
          }
          return p;
        },fs);
  
        if(fs.length) {
          q = " WHERE " + fs.map ( function (d) {
              return "'" + d.name + "' " + d.op + " " +  (Array.isArray(d.value) ? 
                  '(' + d.value.map(function(e) { return parentHandler.makeQuote(e,typeof e === 'number' ? null : 'string')}).join(',') + ')' : 
                    parentHandler.makeQuote(d.value,typeof d.value === 'number' ? null : 'string'));
          })
          .join (" AND ");
        }

      }
      // and the parameters
      if (handleCode === enums.CODE.OK) {
        var paramResult = parentHandler.getQueryParams(queryParams);
        
        handleError = paramResult.handleError;
        handleCode = paramResult.handleCode;
      }
      
      if (handleCode === enums.CODE.OK) {
        q += paramResult.data.map (function (p) {
          
          if (p.param === 'sort') {
            return ' ORDER BY ' + "'" + p.sortKey + "'" + (p.sortDescending ?  ' DESC' : ' ASC') ;
          }
          else if (p.param === 'limit' ){
            return ' LIMIT ' +  p.limit;
          }
          else if (p.param === 'skip' ) {
            return ' OFFSET ' +  p.skip ;
          }
        }).join(' ');

      }        
      result = q;
      
    }
    catch(err) {
      handleError = err + "(query:" + q + ")";
      handleCode =  enums.CODE.DRIVER;
    }
    
    return parentHandler.makeResults (handleCode,handleError,result);
  
  };
  
  /**
   * DriverFusion.query()
   * @param {object} queryOb some query object 
   * @param {object} queryParams additional query parameters (if available)
   * @param {boolean} keepIds whether or not to keep driver specifc ids in the results
   * @return {object} results from selected handler
   */
  self.query = function (queryOb,queryParams,keepIds) {

    return parentHandler.readGuts ( 'query' , function() { 
      return queryGuts_ (queryOb , queryParams , keepIds )
    });

  };
  
  function queryGuts_ (queryOb , queryParams , keepIds ) {
  
    var result =null,sqlString='',driverIds=[],handleKeys=[];
    handleError='', handleCode=enums.CODE.OK;
    
    try {
      var qr = makeQuery(queryOb,queryParams);
      if (handleCode === enums.CODE.OK) {
        // first need to do a desribe as a workaround for not being able to do *,rowid
        var fs,fr;
        if (keepIds) {
          sqlString = "DESCRIBE "+siloId;
          fr = FusionTables.Query.sqlGet(sqlString);
          if (fr && fr.rows) {
            fs = fr.rows.map(function(d) {
              return "'" + d[1] + "'";
            }).join(",");
            fs = "rowid" + (fs ? "," + fs : '');
          }
        }
        
        fs = fs || "*";
        
        sqlString = "SELECT " + fs + " FROM " + siloId + qr.data;
        var options = {hdrs: false};
        fr = FusionTables.Query.sqlGet(sqlString,options);
        // need to convert to key pairs
        if (fr && fr.rows && fr.columns) { 
          result = fr.rows.map(function(r) {
            var o = {};
            fr.columns.forEach(function(c,i) {
              if (c!=='rowid') {
                o[c] = r[i];
              }
              else {
                if (keepIds) {
                  driverIds.push({rowid:r[i]});
                  handleKeys.push(r[i]);
                }
              }
            });

            return o;
          }) ;
        }
        else {
          result = [];
        }
      }
    }
    catch(err) {
      handleError = err + "(query:" + sqlString + ")";
      handleCode =  enums.CODE.DRIVER;
    }
    
    return parentHandler.makeResults (handleCode,handleError,parentHandler.unFlatten(result),keepIds ? driverIds :null,keepIds ? handleKeys:null);
  };
  
  self.removeByIds = function (keys) {

    return parentHandler.writeGuts ( 'removeByIds' , function() {
      
      var result =null,sqlString='';
      handleError='', handleCode=enums.CODE.OK;
      try {
        keys.forEach (function(d) {
          sqlString = "DELETE FROM " + siloId + " WHERE ROWID = '" + d + "'";
          result = parentHandler.rateLimitExpBackoff ( function () { 
            return FusionTables.Query.sql(sqlString); }) ;
        });
      }
      catch(err) {
        handleError = JSON.stringify(err) + "(query:" + sqlString + ")";
        handleCode =  enums.CODE.DRIVER;
      }
      return parentHandler.makeResults (handleCode,handleError,result);
    });
  };
  
  self.remove = function (queryOb,queryParams) {
   
    return parentHandler.writeGuts ( 'remove' , function() {
    
      var result =null,sqlString='';
      handleError='', handleCode=enums.CODE.OK;
  
      try {
        if (queryOb || queryParams) {
          var qr = queryGuts_ (queryOb,queryParams,true);
          if (handleCode === enums.CODE.OK) {
            qr.handleKeys.forEach (function(d) {
              sqlString = "DELETE FROM " + siloId + " WHERE ROWID = '" + d + "'";
              result = parentHandler.rateLimitExpBackoff ( function () { 
                return FusionTables.Query.sql(sqlString); }) ;
            });
          }
        }
        else {
          // delete them all
          var c = self.count();
          if (c.handleCode === enums.CODE.OK && c.data[0].count > 0) {
            sqlString = "DELETE FROM " + siloId;
            result = parentHandler.rateLimitExpBackoff ( function () { 
              return FusionTables.Query.sql(sqlString); 
            } ) ;
          }
        }
      }
      catch(err) {
        handleError = JSON.stringify(err) + "(query:" + sqlString + ")";
        handleCode =  enums.CODE.DRIVER;
      }
      
      return parentHandler.makeResults (handleCode,handleError,result);
      
    });
  };
  
  self.getColumnObj = function () {
    var o ={};
    FusionTables.Column.list(siloId).items.forEach (function(p) { 
        o[p.name]=p;
      });
    return o;
  };
  
  function colsInData_ (obs) {

    // get all columns in the data
    var cols = {};
    obs.forEach (function(u) {
      var d = parentHandler.flatten(u);
      Object.keys(d).forEach(function(c) {
       
        if (cols.hasOwnProperty(c)){
          if (cols[c] === 'undefined') {
            cols[c] = cUseful.isDateObject(d[c]) ? 'DATETIME': typeof d[c];
          }
          else if (cols[c] !== typeof d[c] && typeof d[c] !== 'undefined') {
            // this is a mixed type
            
            cols[c] = 'MIX';  
          }
        }
        else {

          // - its a new one (boolean not available in fusion) .. added object to treat everything else as string
          cols[c] = (typeof d[c] === "boolean" || cUseful.isObject(d[c]) ? "string" : typeof d[c]);
          
        }
      });
    });
    
    return cols;
        
  }
  
  function addMissingCols_ (cols) {
       
    // and those already in the fusion table
    var fCols = self.getColumnObj();
    
    // add any that are missing
    Object.keys(cols).forEach(function(p) {

      if (!fCols || (fCols && !fCols.hasOwnProperty(p))) {
        
        
        var cob = {name:p,type:(cols[p] === 'MIX' || typeof cols[p] === 'undefined') ? "STRING" : cols[p].toUpperCase()};

        var fr = parentHandler.rateLimitExpBackoff ( function () { 
          
          return FusionTables.Column.insert(cob, siloId); 
        } ) ;
        
      }
    });
    
    //refresh
    return self.getColumnObj();
  }
  /**
   * DriverFusion.save()
   * @param {Array.object} obs array of objects to write
   * @return {object} results from selected handler
   */
  self.save = function (obs) {
  
    return parentHandler.writeGuts ( 'save' , function() {
    
      var result =null,q,chunk,sqlString = '',hKeys=[];
      handleError='', handleCode=enums.CODE.OK;

      try {
        // all the columns in the data
        var cols = colsInData_(obs);
        // add any new ones and return full set
        var fCols = addMissingCols_ (cols);

        // now insert the data
        if (!fCols) {
          handleError = "could not create columns" + "(query:" + q + ")";
          handleCode =  enums.CODE.DRIVER;
        }
        else {
          q = obs.map (function(u) {
            var d = parentHandler.flatten(u);
            
            return "INSERT INTO " + siloId + 
              " (" + Object.keys(d).map(function(k){ 
                return "'" + k + "'"; })
                .join(",") + ")" + 
                " VALUES (" + 
                Object.keys(d).map(function(k){ 
                 
                  return parentHandler.makeQuote(parentHandler.escapeQuotes(d[k]),fCols[k].type); 
                })
                .join(",") +")" ;
          }) ;
          
          // save it
          var  p=0;


          while (p < q.length) {
    
            var chunk = [];
            // this is about batching insert statements, but there are lots of quotas
            // we always do at least one, and let fusion deliver the size news if one transaction is too big
            for ( var chunkSize =0 ; chunk.length === 0 || (chunkSize < MAXCHUNKSIZE && chunk.length < MAXINSERTS && p+chunk.length < q.length ) ; p++ ) {
                chunk.push(q[p]);
                chunkSize += q[p].length;
            }
  
            sqlString = chunk.join(";")+";"
            
            // write it
            var r = parentHandler.rateLimitExpBackoff ( function () { 
              return FusionTables.Query.sql(sqlString); 
            } ) ;
            //build up handle keys
            r.rows.forEach(function(d) { hKeys.push(d[0])});
          }  
  
         // now we can delete the dummy column if it exists - we only needed it to create a table
         if ( fCols.hasOwnProperty (DUMMYCOLUMN) && q.length) {
         // cant get this to work
           //FusionTables.Column.remove(fCols[DUMMYCOLUMN].columnId , siloId) ; 
         }
        }
      }
      catch(err) {
        handleError = err + "(query:" + sqlString + ")";
        handleCode =  enums.CODE.DRIVER;
      }
      return parentHandler.makeResults (handleCode,handleError,obs,undefined,hKeys);
      
    });
  };
 

  /**
   * DriverFusion.count()
   * @param {object} queryOb some query object 
   * @param {object} queryParams additional query parameters (if available)
   * @return {object} results from selected handler
   */
  
  self.count = function (queryOb,queryParams) {
      
    return parentHandler.readGuts ( 'count' , function() {
      var result =null, q;
      handleError='', handleCode=enums.CODE.OK;
      
      try {
        q ='', qr = makeQuery(queryOb,queryParams);
  
        if (qr.handleCode !== enums.CODE.OK) {
          handleError = qr.handleError;
          handleCode = qr.handleCode;
        }
        else {
          q = "SELECT COUNT() FROM " + siloId + qr.data;
          var options = {hdrs: false};
          var fr = parentHandler.rateLimitExpBackoff ( function () {  
            return FusionTables.Query.sqlGet(q,options)
          });
          result = [{count:fr && fr.rows ? parseInt(fr.rows[0][0],10) : 0}];
        }
  
      }
      catch(err) {
        handleError = err + "(query:" + q + ")";
        handleCode =  enums.CODE.DRIVER;
      }
  
      return parentHandler.makeResults (handleCode,handleError,result);
    });
  };
  
    
  /**
   * Driver.get()
   * @param {string} key the unique return in handleKeys for this object
   * @param {boolean} keepIds whether or not to keep driver specifc ids in the results
   * @return {object} results from selected handler
   */

  self.get = function (key,keepIds) {
  
    return parentHandler.readGuts ( 'get' , function() {
      var data,hk =[],dk=[];
      // getting multiple in one get doesnt work with fusion, so we need a loop
      try {
        data = (Array.isArray (key) ? key : [key]).map (function (k) {
          var d = parentHandler.isObject(k) ? k.key : k;
          var result = self.query ({rowid:d},undefined, keepIds);
          if (handleCode !== enums.CODE.OK) {
            handleCode = result.handleCode;
            handleError = result.handleError;
          }
          if (keepIds) {
            parentHandler.arrayAppend( hk,  result.handleKeys);
            parentHandler.arrayAppend( dk,  result.driverIds);
          }
  
          return result.data.length ? result.data[0] : null;
        });
      }
      catch(err) {
        handleError = err + "(get key:" + key + ")";
        handleCode =  enums.CODE.DRIVER;
      }
  
      return parentHandler.makeResults (handleCode,handleError,data,dk,hk);
    });
  };
  

  /**
   * Driver.update()
   * @param {string} keys the unique return in handleKeys for this object
   * @param {object} obs what to update it to
   * @return {object} results from selected handler
   */

  self.update = function (keys,obs) {
  
    return parentHandler.writeGuts ( 'update' , function() {
      var result =null,sqlString='';
      handleError='', handleCode=enums.CODE.OK
      // this kind of sucks, since we have to to a separate update for each key
      if(!Array.isArray(keys)) keys = [keys];
      if(!Array.isArray(obs)) obs = [obs];

      // all the columns in the data
      var cols = colsInData_(obs);
      
      // add any new ones and return full set
      var fCols = addMissingCols_ (cols);
        
      try {
        result = keys.map (function(d,i) {
          var ob = parentHandler.flatten(obs.length === 1 ? obs[0] : obs[i]);
          sqlString = "UPDATE " + siloId + " SET " +
            Object.keys(ob).map ( function (k) { 
              return  "'" + k + "'" + " = " + parentHandler.makeQuote(parentHandler.escapeQuotes(ob[k]),fCols[k].type) 
            }).join(",") +
            " WHERE rowid = '" + d + "'";

                
          result = parentHandler.rateLimitExpBackoff ( function () { 
                return FusionTables.Query.sql(sqlString); 
          }) ;
          if (!result || result.length === 0) handleCode = enums.CODE.NOMATCH;
          
        });
      }
      catch(err) {
        handleError = err + "(update key:" + sqlString + ")";
        handleCode =  enums.CODE.DRIVER;
      }
  
      return parentHandler.makeResults (handleCode,handleError,result);
    });
  };
  

  
  return self;
  
}
