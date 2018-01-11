var Store = (function (ns) {

  // constants
  ns.constants = {
    maxThreads:10,
    minChunkSize:128
  };
  
  // to do with plotting the progress of the parallel running items
  ns.progress = {
    width:80,
    height:10,
    borderWidth:1,
    borderFill:"#ffffff",
    fills: {
      wait:'#757575',
      map:'#FF9800',
      failed:'#FF5252',
      reduce:'#536DFE',
      done:'#4CAF50',
      running:'#FFEB3B'
    },
    updateMs:2000
  };
  
  // state variables
  ns.state = {
    finished:false
  };
  

  // data 
  ns.data = {
  
  };
  
  // work info - need this to get started
  ns.orchestrationNamespace = "Orchestration";
    
  ns.work = {};


    
  
  
  return ns;
}) ({});
