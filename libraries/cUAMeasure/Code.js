// how to use
// var ua= new UAMeasure ("UA-45711027-1","EXCEL","bruce")
// ua.postAppView("testingua")
// ... do some stuff
// ua.postAppKill();
function getLibraryInfo () {

  return { 
    info: {
      name:'cUAMeasure',
      version:'2.0.1',
      key:'MIHfxr-fc_7bXa1l0Dkk0oqi_d-phDA33',
    },
    dependencies:[
    ]
  }; 
}
var UAMeasure = function (uaCode,optProperty, optId,optOut,optVersion, optFailSilently) {

  var self = this;
  var pVersion = optVersion || "GAS-v0.1";
  var pFailSilently = (typeof optFailSilently === 'undefined' ?  true : optFailSilently);
  var pPostData ='';
  
  

  self.generateUniqueString = function () {
    
    function arbitraryString (length) {
      var s = '';
      for (var i = 0; i < length; i++) {
        s += String.fromCharCode(randBetween ( 97,122));
      }
      return s;
    }
    
    function randBetween(min, max) {
      return Math.floor(Math.random() * (max - min + 1)) + min;
    }
    return arbitraryString(2) + new Date().getTime();
  };

  
  // you can opt out out of measurement for this instance
  var pOptOut;
  self.setOptOut = function (out) {
    pOptOut = out || false;
  };
  self.setOptOut(optOut);
  
  // this is your property code
  var  pUACode = uaCode;
  
  // this is your property name
  var pProperty = optProperty || 'cUAMeasure';
  
  // this is an id you can use to distinguish repeat visitors. If not specified, a unique code generated for each instance
  var pId = optId || self.generateUniqueString();

  
  // this obfuscates id
  self.obfuscate = function () {
    var salt = "kermit the frog";
    return Utilities.computeDigest( Utilities.DigestAlgorithm.MD2, pId + salt);
  }
  
  // this is univeral analytics url
  var pUrl = "http://www.google-analytics.com/collect";

  self.getProperty = function () {
    return pProperty;
  };
  
  self.getUACode = function () {
    return pUACode;
  };
  
  self.getUrl = function () {
    return pUrl;
  };
  
  self.getVersion = function () {
    return pVersion;
  };

  self.getOptOut = function () {
    return pOptOut;
  };
  
  // this posts an app and should be called to initialize a session
  self.postAppView = function (page) {

    pPostData = "v=1&tid=" + self.getUACode() + "&cid=" +
          self.obfuscate() + "&t=appview&an=" + self.getProperty() + "&av=" + self.getVersion() + "&cd=" + page;

    return self.execute ("start");
    
  };

  // this is called to end session
  
  self.postAppKill = function() {
    return self.execute ("end");
  };
  
  self.execute = function (type) {

    try {
      if (!self.getOptOut() && pPostData) {
        var options = {
          payload: pPostData + "&sc=" + type,
          method: 'POST'
        };
        var p = UrlFetchApp.fetch(self.getUrl(),options);
        return self;
      }
    }
    catch(err) {
      pOptOut = true;
      if (!pFailSilently) throw(err);
    }
    return null;
  }

  
  return self;
};

