/**
 * this only runs client side so no point
 * in defining it server side too
 * use presence of HtmlService to detect Server side
 */
var Client = this.HtmlService ? null : (function(ns) {

      ns.init = function() {
        return new Promise(function(resolve, reject) {
          // first get the work that needs to be done
          Store.state.startedAt = new Date().getTime();

          Provoke.run(Store.orchestrationNamespace, "init")
            .then(function(work) {
              // save this for reference and rendering
              Store.data.work = work;

              // get a unique id for this work
              work.startedAt = Store.state.startedAt;

              // start monitoring
              Render.init();

              // update the monitor with the new stages
              work.stages.forEach(function(d, i) {
                Render.initStage(d);
              });

              // initial stage is now done
              const stage = work.stages[0];
              stage.operation = "done";
              resetCursor();

              Render.stageUpdates();

              // start working on the stages
              return doStages(stage);
            })
            ["catch"](function(err) {
              resetCursor();
              App.showNotification("Failed to get work ", err);
            });

          // this is recursive
          function doStages(cs) {
            const sd = Store.data.work;

            // the latest stage that's been reduced

            const stage = sd.stages[cs.stageIndex + 1];

            // all done
            if (!stage || Store.state.finished) {
              Store.state.finished = true;
              resolve({
                work:sd,
                stage:cs
              });
              return;
            }

            // maybe the data count is not from the previous stage
            stage.itemsIn = (stage.dataCount ? ns.findStage(stage.dataCount) : cs).itemsOut;
            stage.startedAt = new Date().getTime();
            stage.operation = "map";

            // divide the work into chunks
            stage.chunks = chunker(stage);
            Render.stageUpdates();

            // run it the required number of times
            return Promise.all(
              stage.chunks.map(function(d) {
                return Provoke.run(
                  stage.instances.namespace,
                  stage.instances.method,
                  {
                    stageIndex: stage.stageIndex,
                    chunkIndex: d.chunkIndex,
                    stages: sd.stages
                  }
                )
                  .then(function(result) {
                    stage.chunks[result.chunkIndex] = result;
                    Render.stageUpdates();
                    return result;
                  })
                  ["catch"](function(err) {
                    d.error = err;
                    d.finished = true;
                    console.log("failed at chunk", d, err);
                    throw err;
                  });
              })
            )
              .then(function(results) {
                const r = Provoke.run("Server", "reduceStage", stage);
                stage.operation = "reduce";
                Render.stageUpdates();
                return r;
              })
              .then(function(reducedStage) {
                const fs = (sd.stages[reducedStage.stageIndex] = reducedStage);
                fs.operation = "done";
                // this is for debugging only
                if (stage.logReduce ) {
                  Provoke.run("Server", "getFromStore", fs.storeKey).then(
                    function(r) {
                      console.log("values", r);
                    }
                  );
                }
                Render.stageUpdates();
                doStages(fs);
              })
              ["catch"](function(err) {
                resetCursor();
                if (stage) {
                  stage.finished = true;
                  stage.error = err;
                  stage.operation = "failed";
                }
                Store.state.finished = true;
                console.log("failed at stage", stage, err);
                App.showNotification("Failed processing work ", err);
                reject(err);
              });
          }
        });
      };

      function chunker(stage) {
        const ni = stage.itemsIn;
        const mt = stage.maxThreads || Store.constants.maxThreads;
        const mc = stage.minChunkSize || Store.constants.minChunkSize;

        // now work out how many numItems per thread
        if (ni < 1) throw "no work to do";
        if (mt < 1) throw "must be at leat 1 thread available";
        if (mc < 1) throw "chunk size must be at least 1";
        const chunkSize = Math.max(mc, Math.ceil(ni / mt));
        const chunks = [];

        // start and finish points foreach chunk
        var start = 0;
        while (start < ni) {
          const cs = Math.min(chunkSize, ni - start);
          chunks.push({
            start: start,
            chunkSize: cs,
            chunkIndex: chunks.length
          });
          start += cs;
        }
        return chunks;
      }

      /**
       * this stores a result of a work chunk
       * @param {string} name the stage looked for
       * @param {*} result the result to register
       * @return {object} the stage
       */
      ns.findStage = function(name) {
        // find the stage that made the data - as that's the input
        const stage = Store.data.work.stages.filter(function(d) {
          return d.stage === name;
        })[0];
        if (!stage) throw name + "stage is missing";
        return stage;
      };

      function resetCursor() {
        DomUtils.hide("spinner", true);
      }
      function spinCursor() {
        DomUtils.hide("spinner", false);
      }
      return ns;
    })({});
