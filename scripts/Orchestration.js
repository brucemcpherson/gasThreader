var Orchestration = (function (ns) {

  
  // This is called by server to get info on what the work is
  ns.init = function () {
  
    // get the work
    //const work = Job.getJob ("locations");
     const work = Job.getJob ("performance");
    //const work = Job.getJob ("carriers");
    // keep to this pattern for initalization
    return Server.initializeWork (work);
  };
  

  /**
  * define table
  * @param {object} pack (stages, stageIndex, chunkIndex)
  * @return {object} updated chunk
  */
  ns.defineTable = function (pack) {
    
    // get the reduced stage - all the data
    const stages = pack.stages;
    const stage = stages[pack.stageIndex];
    const chunk = stage.chunks[pack.chunkIndex];
    const arg = Utils.clone(stage.instances.arg);
    
    // the previous stage (or I could seeach for its name)
    const prev = pack.stages[pack.stageIndex -1];
    
    // get all the data from the previous stage
    arg.typeDefs = Server.getReduced (prev).result[0];

    // do the work for this stage
    const result = FusionToDB.defineTable (arg);
    
    // find the stage that made the data - as that's the input 
    const dataStage = Server.findStage ("getData" , stages);
    
    // write that chunk - should only be 1 - and set up the data length as input to next stage
    return Server.registerChunk (stage, chunk, result, dataStage.resultLength);
  
  };
  
  /**
  * make sql import statement
  * @param {object} pack (stages, stageIndex, chunkIndex)
  * @return {object} updated chunk
  */
  ns.makeSqlStatements = function (pack) {

    // get the reduced stage - all the data
    const stages = pack.stages;
    const stage = stages[pack.stageIndex];
    const chunk = stage.chunks[pack.chunkIndex];
    
    // we're going to enhance this arg, so take a copy
    const arg = Utils.clone(stage.instances.arg);
    
    // find the stage that made the data - as that's the input 
    const dataStage = Server.findStage (stage.dataCount , stages);
    
    // get the chunk of data that matches what needs to be processed in this this chunk
    arg.data = Server.getSlice (dataStage ,chunk.start , chunk.chunkSize + chunk.start);

    // find the type def
    const mergeStage = Server.findStage ("mergeFields" , stages);
    const mergeReduced = Server.getReduced (mergeStage);

    // add typedefs & data to the args
    arg.typeDefs = mergeReduced.result[0];

    // its possible to skip within the chunk
    
    const files = [];
    arg.skip = arg.skip || 0;

    // loop around at optimized sql sizes
    const stms = [];
    do {
      
      var valChunk = FusionToDB.makeValues (arg);
     
      if (valChunk.length) {
        
        // make SQL insertsmake
        arg.values = valChunk;

        // make SQL inserts
        arg.inserts = FusionToDB.makeInserts (arg);
    
        // write to a sequence of inserts
        stms.push(FusionToDB.makeInsertSql (arg));
        
        arg.skip += valChunk.length;
        
      }
    
    } 
    while (valChunk.length);
    
    // write that chunk - and set up the data length as input to next stage
    return Server.registerChunk (stage, chunk, stms);
  
  };

  /**
  * make drive files for sql input
  * @param {object} pack (stages, stageIndex, chunkIndex)
  * @return {object} updated chunk
  */
  ns.makeSqlScripts = function (pack) {

    // get the reduced stage - all the data
    const stages = pack.stages;
    const stage = stages[pack.stageIndex];
    const chunk = stage.chunks[pack.chunkIndex];
    
    // we're going to enhance this arg, so take a copy
    const arg = Utils.clone(stage.instances.arg);
    
    // get the prev stage
    const prev = stages[stage.stageIndex -1];
    arg.sql =  Server.getSlice (prev ,chunk.start , chunk.chunkSize + chunk.start);
    
    // filename starts at chunk start + some offset if given
    arg.fileNameStart = (arg.fileNameStart || 0 ) + chunk.start ;
    arg.fileNameStem = (arg.fileNameStem || ("sqlins-" + arg.db + "-" + arg.table + "-") ) + zeroPad(chunk.chunkIndex,3) + "-";
    arg.fileExtension = arg.fileExtension || ".sql";

    arg.folderId = arg.folderId || DriveApp.getRootFolder().getId();
    var folder = DriveApp.getFolderById(arg.folderId);
    if (!folder) throw 'folder for id ' + arg.folderId + ' not found';

    const file = folder.createFile(folder.createFile(arg.fileNameStem+zeroPad(arg.fileNameStart,6)+arg.fileExtension, arg.sql.join("\n"), "text/plain"));


    // write that chunk
    return Server.registerChunk (stage, chunk, file.getId());
  };
  
   /**
  * write sql scripts to a directory for the job
  * @param {object} pack (stages, stageIndex, chunkIndex)
  * @return {object} updated chunk
  */
  ns.zipSqlScripts = function (pack) {

    // get the reduced stage - all the data
    const stages = pack.stages;
    const stage = stages[pack.stageIndex];
    const chunk = stage.chunks[pack.chunkIndex];
    
    // we're going to enhance this arg, so take a copy
    const arg = Utils.clone(stage.instances.arg);
    
   // get the chunk
   // the previous stage (or I could seeach for its name)
    const prev = stages[stage.stageIndex -1];
    arg.files =  Server.getReduced (prev).result;  
    
    // get the table def
    const tableStage = Server.findStage ("defineTable" , stages);
    const tableReduced = Server.getReduced (tableStage);
 
    // filename starts at chunk start + some offset if given
    arg.fileExtension = arg.fileExtension || ".sql";
    arg.tableFileName = arg.tableFileNameStem || ("sqltable-" + arg.db + "-" + arg.table);
    arg.folderId = arg.folderId || DriveApp.getRootFolder().getId();
    var folder = DriveApp.getFolderById(arg.folderId);
    if (!folder) throw 'folder for id ' + arg.folderId + ' not found';
    const tableFile = arg.tableCreate ? 
      folder.createFile(arg.tableFileName+arg.fileExtension, tableReduced.result[0], "text/plain") : null;
 
    // get and zip files
    const blobs = arg.files.reduce (function (p,c) {
      const file = DriveApp.getFileById(c);
      p.push(DriveApp.getFileById(c).getBlob());
      return p;
    } ,tableFile ? [tableFile.getBlob()] : []);
    
    // write the zip file
    const zipFile = folder.createFile(Utilities.zip (blobs , stage.workId + ".zip"));
    
    // delete the input files
    arg.files.forEach (function (d) {
      DriveApp.removeFile(DriveApp.getFileById(d));
    });
    
    // report on what just happened
    return Server.registerChunk (stage, chunk, {
      filesInZip:blobs.map(function (d) {
        return d.getName();
      }),
      zip:{
        name:zipFile.getName(),
        id:zipFile.getId()
      } 
    });
  
  };
  
  function zeroPad(num, numZeros) {
    var n = Math.abs(num);
    var zeros = Math.max(0, numZeros - Math.floor(n).toString().length );
    var zeroString = Math.pow(10,zeros).toString().substr(1);
    if( num < 0 ) {
      zeroString = '-' + zeroString;
    }
    
    return zeroString+n;
  }
  
  
 /**
  * we have to merge several definitions
  * @param {object} pack (stages, stageIndex, chunkIndex)
  * @return {object} updated chunk
  */
  ns.mergeFields = function (pack) {
  
    // get the reduced stage - all the data
    const stage = pack.stages[pack.stageIndex];
    const chunk = stage.chunks[pack.chunkIndex];
    
    // the previous stage (or I could seeach for its name)
    const prev = pack.stages[pack.stageIndex -1];
    
    // get all the data from the previous stage
    const reduced = Server.getReduced (prev);
    
    
    // merge the fields
    const fields = reduced.result.reduce (function (p,c) {
      Object.keys(c)
      .forEach (function(k) {
        if (!p[k]) {
          p[k] = c[k];
        }
        else {
          /// merge the field definition
          if (c[k].type !== p[k].type) {
            // discovered mixed types
            if (p[k].type === "INT" && c[k].type === "FLOAT") {
              p[k].type = "FLOAT";
            }
            else {
              p[k].type === "STRING";
            }
            p[k].size = Math.max (p[k].size , c[k].size);
            p[k].nulls += c[k].nulls;
          }
        }
      });
      return p;
    },{});
    
    // write that chunk
    return Server.registerChunk (stage, chunk, fields);
  };
  
 /**
  * we have to merge several definitions
  * @param {object} pack (stages, stageIndex, chunkIndex)
  * @return {object} updated chunk
  */
  ns.defineFields = function (pack) {
   
    // get the reduced stage - all the data
    const stage = pack.stages[pack.stageIndex];
    const chunk = stage.chunks[pack.chunkIndex];
    // we're going to enhance this arg, so take a copy
    const arg = Utils.clone(stage.instances.arg);
    
    // the results from the previous stage (or I could seeach for its name)
    const prev = pack.stages[pack.stageIndex -1];

    // normally prev.result would contain the reduced data from the previous stage 
    // but this takes care of the chunks being different sizes
    arg.data = Server.getSlice (prev ,chunk.start , chunk.chunkSize + chunk.start);
    
    // look at all the data and figure out the field types
    const result = FusionToDB.defineFields (arg);
    
    
    // register for this chunk
    return Server.registerChunk (stage, chunk, result, 1);
  
  };
  
  /**
  * @param {object} pack (stages, stageIndex, chunkIndex)
  * @return {object} updated chunk
  */
  ns.getData = function (pack) {
    
    // get the reduced stage - all the data
    const stage = pack.stages[pack.stageIndex];
    const chunk = stage.chunks[pack.chunkIndex];
    
    // Important
    // if you need to modify arg - clone it first
    const arg = Utils.clone(stage.instances.arg);
    
    // in this case there's no data to get,
    // as the previous stage was just a count
    
    // arg contains the info needed to open the db
    // add the slice I'm doing
    arg.maxr = chunk.chunkSize;
    arg.start = chunk.start;
    
    // there's other chunking that happens in Fusion too
    // as there's a limit to how much can be read in one go
    // that's dealt with invisibly here
    const result = FusionToDB.getChunked (arg);

    // register it for future reference
    return Server.registerChunk (stage, chunk, result);
  
  };
  
  /**
  * @param {object} pack (stages, stageIndex, chunkIndex)
  * @return {object} updated chunk
  */
  ns.getCount = function (pack) {
   
    const stage = pack.stages[pack.stageIndex];
    const chunk = stage.chunks[pack.chunkIndex];
    const arg = stage.instances.arg;
    
    // do the work - any arguments will be as below
    const count = FusionToDB.getCount (arg)[0].count;
    
    // register it for future reference
    // normally the count of items will be the length of result, but it can
    // be overridden by a number or a function in the 4th argument
    return Server.registerChunk (stage, chunk , [count] , arg.maxr ? Math.min(count,arg.maxr) : count );

  };


  return ns;
}) ({});

