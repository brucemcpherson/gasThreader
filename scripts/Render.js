/**
 * this only runs client side so no point
 * in defining it server side too
 * use presence of HtmlService to detect Server side
 */
var Render = this.HtmlService ? null : (function (ns) {

  ns.init = function () {

    const du = DomUtils;
    const el = du.elem;
    const dad = du.addElem;
    const se =  {} ;
    Store.state.elements = [se];
    el("work-title").innerHTML = Store.data.work.title;
    el("work-id").innerHTML = Store.data.work.workId;
    el ("work-table").innerHTML = "";
    const tab = dad ( el ("work-table") , "table", "", "mui-table");
    const thead = dad (tab, "thead");
    var tr = dad (thead , "tr");
    // headings
    dad (tr , "th", "Run time");
    dad (tr , "th" , "Stage");
    dad (tr , "th" , "Operation");
    dad (tr , "th", "Run time");

    dad (tr , "th" , "Items in");
    dad (tr , "th" , "Items out");
    dad (tr , "th" , "What");
    dad (tr , "th" , "Threads");
    dad (tr , "th" , "Progress");
    Store.state.tbody = dad (tab , "tbody");
    
    // do regular updates
    
    var t = d3.timer(function() {
      if (Store.state.finished)t.stop();
      ns.stageUpdates();
    },Store.progress.updateMs);

    return ns;

  };
  
  ns.initStage = function (stage) {
    const se =  {};
    
    const du = DomUtils;
    const dad = du.addElem;
    const tr = dad (Store.state.tbody ,"tr");
    
    se.runningFor = dad( tr , "td");
    se.stage = dad( tr , "td");
    se.operation = dad (tr , "td");
    se.runTime = dad( tr , "td");
    se.itemsIn = dad( tr , "td");
    se.itemsOut = dad( tr , "td");
    se.what = dad( tr , "td");
    se.chunks = dad( tr , "td"); 
    se.progress = dad( tr , "td");
    se.selection = ns.initProgress(se);
    Store.state.elements[stage.stageIndex] = se;
    return ns;
  };
  
  ns.stageUpdates = function () {
 
    Store.data.work.stages.forEach (function (d,i) {
      if (Store.state.elements[i]){
        ns.stageUpdate (d);
      }
    });
    return ns;
  }
  
  // when a stage is starting report on progress
  ns.stageUpdate = function (stage) {
 
    const se = Store.state.elements[stage.stageIndex] ;
    const now = new Date().getTime();
    if (!stage.finished && Store.state.startedAt) {
      se.runningFor.innerHTML =  Math.round( (now - Store.state.startedAt  )/1000);
    }
    se.stage.innerHTML = stage.stageTitle;
    se.operation.innerHTML = stage.operation;
    if (!stage.finished && stage.startedAt) {
      se.runTime.innerHTML =  Math.round((now- stage.startedAt  )/1000);
    }

    se.itemsIn.innerHTML = stage.itemsIn || "";
    se.itemsOut.innerHTML = stage.resultLength || ""; 
    if (stage.instances) se.what.innerHTML = [stage.instances.namespace,stage.instances.method].join("."); 
    if (stage.chunks) se.chunks.innerHTML = stage.chunks.length;
    ns.updateProgress (stage);
    return ns;
  }
  
  ns.showResult = function (mess) {
    const du = DomUtils;
    const el = du.elem;
    el("result-panel").innerHTML = mess;
    return ns;
  }
  
  
  // init a chunk progress
  ns.initProgress = function (se) {

    const sp = Store.progress;
    // we'll need an svg element
    // with a group below it to contain whatever items we're going to add
    return d3.select (se.progress)
      .append("svg")
        .attr("width", sp.width)
        .attr("height", sp.height);
  
  };
    
  
  ns.updateProgress = function (stage) {
    const se = Store.state.elements[stage.stageIndex] ;
    const sp = Store.progress;
    const data = (stage.chunks || []).map (function(d){
      return {
        chunk:d,
        height:sp.height,
        width:sp.width / stage.chunks.length, 
        x:d.chunkIndex /stage.chunks.length*sp.width,
        y:0,
        fill:d.finished ? (d.error ? sp.fills.failed : sp.fills[stage.operation]) : sp.fills.running
      }
    });

    const boxes = se.selection
      .selectAll(".progress-group")
      .data(data);
    boxes.exit().remove();
    const benter = boxes.enter()
      .append("g")
      .attr("class", ".progress-group");
  
    benter.append("rect").attr("class", "progress-rect");
    // maybe later
    benter.append("text").attr("class", "progress-text");

    const menter = benter.merge (boxes);
    
    menter.select (".progress-rect")
      .attr ("width", function (d) {
        return d.width
      })
     .attr ("height", function (d) {
        return d.height
      })
     .attr ("x", function (d) {
        return d.x
      })
      .attr ("y", function (d) {
        return d.y
      })
      .style ("fill",function (d) {
        return d.fill;
      })
      .style ("stroke",Store.progress.borderFill)
      .style ("stroke-width",Store.progress.borderWidth);
      
      
      
    return ns;


  };
  return ns;
}) ({});
