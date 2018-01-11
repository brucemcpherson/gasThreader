
/** need to modify the manifest file to include
 * scope for fusion tables
 * "oauthScopes": [
 *  "https://www.googleapis.com/auth/fusiontables.readonly"
 * ]
 *
 * Im not connecting to the DB directly as it would take too long
 * instead - writing .sql files for batch load to cloud storage
 */
var FusionToDB = (function (ns)  {

  
  // do a start to finish conversion
  ns.completeBuild = function  (options) {
  
    // get the fusion data
    options.data = ns.getChunked (options);
    
    // get the type defs
    options.typeDefs = ns.defineFields (options);
  
    // define tables
    const tableDefs = ns.defineTable (options);
    
    // extract the values
    var chunk;
    const files=[];
    options.skip = options.skip || 0;
    
    // this is where the sql files will be written to Drive
    options.fileNameStart = options.fileNameStart || 0;
    options.fileNameStem = options.fileNameStem || ("sqlinserts_" + options.db + "_" + options.table + "_");
    options.fileExtension = options.fileExtension || ".sql";
    if (!options.folder) throw 'wont be able to write to drive without a folder';
    
    do {
      
      chunk = ns.makeValues (options);
      if (chunk.length) {
        
        // make SQL inserts
        const inserts = ns.makeInserts ({
          typeDefs:options.typeDefs,
          values:chunk
        });
        
        // turn that into an sql 
        const insertSql = ns.makeInsertSql ({
          db:options.db , 
          table:options.table ,
          inserts:inserts
        });
        
        // write to a sequence of files
        files.push (options.folder.createFile(options.fileNameStem+zeroPad(options.fileNameStart+files.length,6)+options.fileExtension, insertSql, "text/plain"));
        options.skip += chunk.length;
        
      }
    
    } 
    while (chunk.length);
    return files;
    
  };
  
  // make sql insert statements
  ns.makeInserts = function (options) {

    const def = options.typeDefs;
    const values = options.values;

    return values.map (function (d) {
      return "(" + Object.keys (def).map (function (e,i) {
          const value = d[i].toString();
          if (def[e].type === "BOOLEAN") {
            return value.toUpperCase();
          }
          else {
            return ["FLOAT","INT"].indexOf(def[e].type) === -1 ?  "'" + value + "'" : value;
          }
      }).join (",") + ")"
    });
  
  };
  
  // format as insert sql statements
  ns.makeInsertSql = function (options) {
    const database = options.db;
    const tableName = options.table;
    const inserts = options.inserts;
    return "INSERT INTO " + database + "." + tableName + " VALUES\n" +
      inserts.join (",\n") + ";\n"
  };

  ns.getCount = function (options) {
     const tableName = options.fusionTable;
     const fusionKey = options.fusionKey;
     const fusionHandler = ns.getHandler (tableName , fusionKey );
     const result = fusionHandler.count();
     if (result.handleCode < 0) throw result.handleError;
     return result.data;
  };
  
  
  // convert values to input to sql
  ns.makeValues = function (options) {
  
    const limit = options.limit;
    const skip = options.skip || 0;
    const data = options.data;
    const noMax = !limit;
    const def = options.typeDefs;
        
    // just work on a slice
    const chunk = !skip && noMax ? data : 
      (noMax ? data.slice (skip) : data.slice (skip, skip + limit));
    
    // convert to an array of values
    const v = chunk.map (function (d) {
      return Object.keys (def).map (function (e) {
        return ns.convertType (def[e] , d[e] );
      });
    });
    
    return v;
    
  
  }
  
    
  ns.convertType  = function (field, value) {
  
    if (field) {
      if (field.type === "INT") {
        return parseInt (value , 10);
      }
      else if (field.type === "FLOAT") {
        return parseFloat (value);
      }
      else if (field.type === "STRING" || field.type === "TIMESTAMP") {
        return value.toString ? value.toString() : "";
      }
      else if (field.type === "BOOLEAN") {
        return value ? true : false;
      }
      else {
        throw 'unknown field type ' + field.type;
      }
      
    }
    else {
      return value;
    }
  
  }

  
  // have a stab at defining the output table
  // it will need tweaked for primary keys etc.
  ns.defineFields = function (options) {
    
    const model = options.data;
    const forceMap = options.forceMap || {};
   
    
    // look at each row - and deduce fields if not given
    return model.reduce (function (p,c) {
      // look at each item in each row
      Object.keys (c).filter(function (k) {
        return forceMap[k] !== "SKIP";
      })
      .forEach (function (k) {
        
        // final result is an object with a ley and a type && how many nulls there are
        p[k] = p[k] || {
          type:forceMap[k],
          nulls:0,
          size:0
        };

        // work out the size & if there are nulls
        const value = c[k];
        const vs = value.toString();
        if (!vs.length) p[k].nulls++;
        p[k].size = Math.max (p[k].size , vs.length); 
       
        // need to deduce
        if (!forceMap[k]) {
          
          // get the type of the value
          const t = (typeof value).toUpperCase();
          
          // if its a number, we need to decide if this field will be float or int
          if ( t === "NUMBER" && p[k].type !== "STRING") {
            const n = value.toString();
            const nt = n.replace(".","") === n ? "INT" : "FLOAT";
            p[k].type = p[k].type || nt;
            // just make it a float
            if (p[k].type !== nt ) p[k].type === "FLOAT";
          }
        
          else {
            p[k].type = p[k].type || t;
          }
        }
        
      });
      return p;
    },{});

  }
  
  // make an sql to define a table
  ns.defineTable = function(options) {
  
    const database = options.db;
    const tableName = options.table;
    const def = options.typeDefs;

    return "DROP TABLE IF EXISTS "  + database + "." + tableName + " CASCADE;\n" +
      "CREATE TABLE " + database + "." + tableName + " (\n" +
      Object.keys (def).map (function (d) {
        const ds = def[d].type === "STRING" && def[d].size ? "(" + def[d].size + ")" : "" ;
        const dn = def[d].nulls ? "" : " NOT NULL";
        return "  " + d + " " + def[d].type + ds + dn;
      }).join(",\n") + "\n);"
  
  }


  
  // this'll need to be chunked
  ns.getChunked = function (options){
    
     const tableName = options.fusionTable;
     const maxr = options.maxr;
     const fusionKey = options.fusionKey;
     
     const fusionHandler = ns.getHandler (tableName , fusionKey );
     
     // let's get this in chunks of 5000 rows, or a max
     const limit = options.chunkSize || 5000;
     const startAt = options.startAt || 0;
     
     const chunked = [];
     const noMax = !maxr;
     var result;
     do {
       result = fusionHandler.query(null, {
        skip:chunked.length + startAt,
        limit:noMax ? limit : Math.min ( maxr - chunked.length , limit ) 
      });
      if (result.handleCode < 0) throw result.handleError;
      Array.prototype.push.apply (chunked , result.data);
     } while (result.data.length && (noMax || chunked.length < maxr));
     
     return chunked;
  }
  
  function zeroPad(num, numZeros) {
    var n = Math.abs(num);
    var zeros = Math.max(0, numZeros - Math.floor(n).toString().length );
    var zeroString = Math.pow(10,zeros).toString().substr(1);
    if( num < 0 ) {
      zeroString = '-' + zeroString;
    }
    
    return zeroString+n;
  }

  // get a handler for fusion
  ns.getHandler = function  (tableName , fusionKey ) {
  
    // open fusion table
    const fusionHandler = new cDbAbstraction.DbAbstraction (cDriverFusion, {
          dbid:tableName,
          siloid:fusionKey
    });
    if (!fusionHandler.isHappy()) throw 'unable to open fusion table:' +  fusionKey + '/' + tableName ; 
    return fusionHandler;
  }
  
  return ns;
})({});
