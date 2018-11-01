var Server = (function (ns) {
  const LIFE = 60 * 1 * 60;   // 1 hour results life
  // always keep one chunk around to avoid getting it
  ns.currentChunk = null;
  
  
  /**
   * this can be used to get a slice of a stage
   * @param {object} stage a stage object
   * @param {number} startSlice start slice param
   * @param {number} endSlice end slice param
   * @return {[*]} a slice of the stage data
   */
  ns.getSlice = function (stage , startSlice , endSlice) {
   
    // keeo a current chunk in cache to avoid getting it if possible
    const cv = ns.currentChunk;
    const cc = cv && cv.chunk;
    
    // the chunks for the required stage
    const sc = stage.chunks;
    
    // defaults for slicing are the stage extents
    startSlice = startSlice || 0;
    endSlice = endSlice || stage.resultLength;
    
    // first identify if the required chunk is the current chunk in memory
    const tv = cc && sc[cc.chunkIndex] && sc[cc.chunkIndex].storeKey === cc.storeKey  ? cv : null;
    const tc = tv && tv.chunk;
    
    // now see if its entirely in scope
    if (tc && startSlice >= tc.resultStart && endSlice <= tc.resultStart + tc.resultLength) {
      return tv.result.slice (startSlice - tc.resultStart , endSlice - tc.resultStart);
    }
    
    // so either its not in memory, or its the wrong one
    const cf = sc.filter (function (d) {
      return startSlice < d.resultStart + d.resultLength && endSlice >= d.resultStart ;
       
    });
    if (!cf.length) throw 'there are no chunks available for range ' + startSlice + "," + endSlice;
    

    // the required fits
    const deb = [];
    const r = cf.reduce (function (p,c) {
      // need to get the matching chunks from store
      // but dont bother if already in memory
      const vv = tc && c.storeKey === tc.storeKey ? tv : ns.getFromStore (c.storeKey);
      if (!vv) throw 'couldnt find chunk ' + c.storeKey + ' in store';
      
      // now vc should be either a chunk newly from store or the already in memory one, make that the default for future
      ns.currentChunk = {
        result:vv.result,
        chunk:c
      };
      
      // append the part of this chunk that's needed
      // start will be the startSlice - 
      //const stc = Math.max(0, startSlice - c.resultStart);
      //const stf = Math.min(c.resultLength  , endSlice - c.resultStart);
      const stc = Math.max (0 , startSlice - c.resultStart);
      const stf = Math.min (endSlice - c.resultStart , c.resultLength);
      Array.prototype.push.apply (p, vv.result.slice (stc , stf));
      deb.push ({c:c , stc:stc, stf:stf , s:startSlice , e:endSlice, pl:p.length, vr:vv.result.length});
      return p;
    },[]);
    
    // now check that  the slice size matches what was requested
    if (r.length !== endSlice - startSlice) {
     console.log (JSON.stringify(deb));
      throw startSlice + "," + endSlice + ':slice asked for was '  + (endSlice - startSlice) + ' but found ' + r.length;
    }
    return r;
  };
  /**
  * get the work that needs to be done
  * this is mainly just a wrapper 
  * as the Work function would be specific
  * to the thing to be dome
  */
  ns.getCrusher = function () {
    
    // ony intiialize if we dont have on already
    return ns.crusher ||  (ns.crusher = new Squeeze.Chunking ()
    .setStore (CacheService.getUserCache())
    .setChunkSize(100000)   
    .setPrefix ("ps")
    .setMakeDigest (false)
    .funcWriteToStore(function (store, key , str) {
      return Utils.expBackoff(function () { 
          return store.put (key , str , LIFE ); 
      });
    })
    .funcReadFromStore(function (store, key) {
      return Utils.expBackoff(function () { 
        return store.get (key); 
      });
    })
    .funcRemoveObject(function (store, key) {
      return Utils.expBackoff(function () { 
        return store.remove (key); 
      });
    }));
    
  };
  
  
  /**
   * this reduces the chunks of a stage
   */

  ns.reduceStage = function (stage) {
    
    // accumulate the counts
    stage.itemCount = 0;
    stage.resultLength = 0;
    stage.itemsOut = 0;
    stage = stage.chunks.reduce (function (p,c) {
      // need to adjust the resultStart for the chunk
      c.resultStart = p.resultLength;
      p.itemCount +=  c.itemCount;
      p.resultLength += c.resultLength;
      p.itemsOut += c.itemsOut;
      return p;
    }, stage);
    
        
    // if its a skip reduce, then all we have to do is register a special summary item
    if (stage.skipReduce ) {
      return ns.register (stage, []);
    }
    
    else {
      // get all the chunks
      const r = stage.chunks.reduce (function (p,c) {
      
        // get the item from store
        const v = ns.getFromStore (c.storeKey);
        if (!v) throw 'Stage ' + stage.stageTitle + ' is missing chunk ' + c.chunkIndex + ' from store (' +c.storeKey +')';
        Array.prototype.push.apply ( p, v.result);
        return p;

      } ,[]);
      if (stage.resultLength !== r.length) {
        throw 'reduction gave unexpected results length (' + stage.stageTitle + "/" + stage.resultLength + "/" + r.length +')';
      }
      
      return ns.register (stage, r );
    }
    
  };
  
    /**
   * this stores a result of a work chunk
   * @param {object} stage this is the stage definition from the client
   * @param {object} chunk this is the chunk definition from the client
   * @param {[*]} result the result to register
   * @param {number} [itemCount] how many items tosay they are (default is length of result), can be a function
   * @return {object} the updated options
   */
  ns.registerChunk = function ( stage , chunk ,  result, itemCount) {

    result = typeof result === typeof undefined ? [] : result;
    if (!Array.isArray (result)) result = [result];
    chunk.itemCount = itemCount || 0;
    chunk.finished=true;
    chunk.registeredAt=new Date().getTime();
    chunk.stageIndex = stage.stageIndex;
    chunk.resultLength = (result && result.length) || 0;
    chunk.itemsOut = typeof itemCount === "number" ? itemCount : chunk.resultLength;
    // note that we cant know chunk.resultStart at this point
    // as its created by reduce.
    // so always reference the stage item for up to date chunk info
    return  ns.putToStore (chunk , {
      result:result
    });
    
  };
  
  /**
   * this stores a result of a stage
   * @param {object} options this is the work definition from the client
   * @param {[*]} result the result to register
   * @return {object} the updated options
   */
  ns.register = function ( stage ,  result  ) {

    stage.finished = true;
    stage.registeredAt = new Date().getTime();

    return ns.putToStore (stage , {
      stage:stage, 
      result:result
    });

  };
  
  /**
   * this stores a result of a work chunk
   * @param {string} name the stage looked for
   * @param {[object]} stages teh known stages
   * @param {*} result the result to register
   * @return {object} the stage
   */
  ns.findStage = function (name, stages) {
  
    // find the stage that made the data - as that's the input 
    const stage = stages.filter (function (d) {
      return d.stage === name;
    })[0];
    if (!stage) throw name + 'stage is missing';
    return stage;
  };
  
    /**
   * this stores a result of a work chunk
   * @param {object} stage this is the work definition from the client
   * @param {*} result the result to register
   * @return {object} the updated options
   */
  ns.getReduced = function (stage) {
    if (!stage.storeKey) throw 'missing storeKey getting register ' + JSON.stringify(stage);
    if (stage.skipReduce) throw 'reduce stage was skipped - set skipReduce to true or use Server.getSlice to get individual chunks';
    return ns.getFromStore (stage.storeKey);
  };
  
  /**
   * this stores a result of a work chunk
   * @param {object} options this is the work definition from the client
   * @param {*} result the data to register
   * @return {string} the register result
   */
   ns.putToStore = function (options , data ) {

     // get a key
     options.storeKey = options.hasOwnProperty ("chunkIndex") ?
       ns.makeKey (options , options.chunkIndex) : ns.makeKey (options);
     
     // write to store
     ns.getCrusher().setBigProperty (options.storeKey,data);
     // return key for later
     return options;
   };


  /**
   * call this to get going
   * emulates a chunk completion
   */
  ns.initializeWork = function (userWork) {    
    
    // for now restart is not implemented
    // but the presence of a workId would mean a restart
    
    // clone the work, as it's going to be enhanced here
    const work = Utils.clone (userWork);
    
    // for now generate a new one
    work.workId =  Utils.generateUniqueString();
    
    // set the stage indexes and initial states
    work.stages.forEach (function (d,i) {
      d.stageIndex = i;
      d.operation = "wait";
      d.workId = work.workId;
    });
    
    // the first stage is about initialization 
    const stage = work.stages[0];
    
    // initial stage values
    stage.finished = false;
    stage.startServerTime = new Date().getTime();
    stage.stageIndex = 0;
    stage.operation = "init";

    // chunks
    stage.chunks = [{
      start:0,
      chunkSize:1,
      chunkIndex:0
    }];
    
    
    // register
    ns.registerChunk (stage, stage.chunks[0], [], 1);

    // do a reduce
    stage = ns.reduceStage (stage);
    
    // the work package
    return work;
  };
    
  /**
   * this gets a result of a work chunk
   * @param {string} key the key digest previously supplied
   * @return {*} the register result
   */
   ns.getFromStore = function (key ) {
     
     // read from store
     return ns.getCrusher().getBigProperty (key)

   };
   
   /**
    * makes a key for this specific chunk
    * @param {object} options comes from the client
    * @param {object} data what'll be written
    * @return {string} key a key for this item
    */
  ns.makeKey = function (options,ci ) {
    // if no chunkindex , then its a summary
    return typeof ci === typeof undefined ? 
      Utils.keyDigest (options.workId, options.stageIndex ) :
      Utils.keyDigest ( options.workId, options.stageIndex , ci);  
  };
  return ns;
}) ({});
      
