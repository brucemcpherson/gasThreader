/**
 * parametes to describe the work that needs to be done
 * this example contains parameters for multiple similat jobs
 */
var Job = (function(ns) {
  ns.getJob = function(job) {
    // params for this job
    const fusion = ns.jobs[job];
    if (!fusion) throw "unknown job " + job;
    return ns.getWork(fusion);
  };
  
   
  ns.jobs = {
    locations: {
      fusionTable: "airports",
      fusionKey: "1Ug6IA-L5NKq79I0ioilPXlojEklytFMMtKDNzvA",
      db: "airports",
      table: "locations",
      limit: 100,
      folderId: "1h7qeysh2WzDuCNEJh6_qcqY_fTeHcByw",
      maxr: 0,
      tableCreate:true,
      forceMap:{
        "RowId":"SKIP"
      }
    },

    performance: {
      fusionTable: "US Air Carrier Flight Delays-On_Time_Performance",
      fusionKey: "1aoTFJygMLOD0r-QsaKG5-4375VwQpwuGb18dMGw",
      db: "airports",
      table: "performance",
      limit: 10000,
      folderId: "1h7qeysh2WzDuCNEJh6_qcqY_fTeHcByw",
      maxr: 0,
      tableCreate:true,
      forceMap:{
        "ArrDelayMinutes" : "INT",
        "DayofMonth": "INT",
        "DepDelayMinutes" : "INT",
        "Month": "INT",
        "Year" :"INT",
        "DepDelayMinutes":"INT",
        "FlightDate":"TIMESTAMP",
        "isLateDepart":"BOOLEAN",
        "RowId":"SKIP"
      }
    },

    carriers: {
      fusionTable: "carriercodes",
      fusionKey: "1pvt-tlc5z6Lek8K7vAIpXNUsOjX3qTbIsdXx9Fo",
      db: "airports",
      table: "carriers",
      limit: 100,
      folderId: "1h7qeysh2WzDuCNEJh6_qcqY_fTeHcByw",
      maxr: 0,
      tableCreate:true,
      forceMap:{
        "RowId":"SKIP"
      }
    }
  };

  ns.getWork = function(fusion) {
    return {
      title: "convert from fusion table " + fusion.table,
      // future versions - a work id can be specified to rerun
      // for now a new one will be generated each time
      workId: "",
      stages: [
        {
          stage: "init",
          stageTitle: "Initialization",

          instances: {
            namespace: "Orchestration",
            method: "init"
          }
        },
        {
          stage: "count",
          stageTitle: "Counting data",

          instances: {
            namespace: "Orchestration",
            method: "getCount",
            arg: fusion
          }
        },
        {
          stage: "getData",
          stageTitle: "Getting data",

          // normally there's a reduce stage after each map
          // but if the data is too big, you can do a fake reduce
          // that only reduce links to the chunks, rather than copy it
          // and use Server.getSlice to retrieve sections of data for a stage
          skipReduce: true,

          // the maximum number of chunks to break the job into omit for the default
          maxThreads: 24,

          // the min size of a chunk to avoid splitting job into inefficiently small chunks
          // omit for default
          minChunkSize: 200,

          // the
          instances: {
            namespace: "Orchestration",
            method: "getData",
            arg: fusion
          }
        },
        {
          stage: "defineFields",
          stageTitle: "Define fields",

          // the maximum number of chunks to break the job into omit for the default
          maxThreads: 24,

          // the min size of a chunk to avoid splitting job into inefficiently small chunks
          // omit for default
          minChunkSize: 100,

          logReduce:false, 
          
          instances: {
            namespace: "Orchestration",
            method: "defineFields",
            arg:fusion
          }
          
        },
        {
          stage: "mergeFields",
          stageTitle: "Merge fields",

          // the maximum number of chunks to break the job into omit for the default
          maxThreads: 1,

          instances: {
            namespace: "Orchestration",
            method: "mergeFields"
          }
        },
        {
          stage: "defineTable",
          stageTitle: "Define table",

          // the maximum number of chunks to break the job into omit for the default
          maxThreads: 1,

          instances: {
            namespace: "Orchestration",
            method: "defineTable",
            arg: fusion
          },
          
          logReduce:false
        },
        {
          stage: "sqlInserts",
          stageTitle: "Sql inserts",

          // the maximum number of chunks to break the job into omit
          maxThreads: 24,

          // the minimum chunk size should try to match the limit size
          // for how many inserts to do in 1 sql file
          minChunkSize: fusion.limit,

          // where to get the num items to work on
          dataCount: "getData",

          // dont bother reducing - there may be too many to deal with
          // all at once
          skipReduce: true,
          
          instances: {
            namespace: "Orchestration",
            method: "makeSqlStatements",
            arg: fusion
          }
        },
        {
          stage: "sqlScripts",
          stageTitle: "Make sql scripts",

          // the maximum number of chunks to break the job into omit
          maxThreads: 24,

          // the minimum chunk size 
          minChunkSize: 15,

          instances: {
            namespace: "Orchestration",
            method: "makeSqlScripts",
            arg: fusion
          }
        },
        {
          stage: "zipScripts",
          stageTitle: "zip sql package",

          // the maximum number of chunks to break the job into omit
          maxThreads: 1,

          instances: {
            namespace: "Orchestration",
            method: "zipSqlScripts",
            arg: fusion
          }
        }
      ]
    };
  };


  return ns;
})({});
