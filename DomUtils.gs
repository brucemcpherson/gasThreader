/**
 * this only runs client side so no point
 * in defining it server side too
 * use presence of HtmlService to detect Server side
 */
var DomUtils = this.HtmlService ? null :  (function(ns) {
  
  /**
  * converts an svg string to base64 png using the domUrl
  * @param {string} svgText the svgtext
  * @param {number} margin the width of the border - the image size will be height+margin by width+margin
  * @return {Promise} a promise to the bas64 png image
  */
  ns.svgToPng = function (svgText, margin,fill) {
    // convert an svg text to png using the browser
    return new Promise(function(resolve, reject) {
      try {
        // can use the domUrl function from the browser
        var domUrl = window.URL || window.webkitURL || window;
        if (!domUrl) {
          throw new Error("(browser doesnt support this)")
        }
        
        // figure out the height and width from svg text
        var match = svgText.match(/height=\"(\d+)/m);
        var height = match && match[1] ? parseInt(match[1],10) : 200;
        var match = svgText.match(/width=\"(\d+)/m);
        var width = match && match[1] ? parseInt(match[1],10) : 200;
        margin = margin || 0;
        
        // it needs a namespace
        if (!svgText.match(/xmlns=\"/mi)){
          svgText = svgText.replace ('<svg ','<svg xmlns="http://www.w3.org/2000/svg" ') ;  
        }
        
        // create a canvas element to pass through
        var canvas = document.createElement("canvas");
        canvas.width = height+margin*2;
        canvas.height = width+margin*2;
        var ctx = canvas.getContext("2d");
        
        
        // make a blob from the svg
        var svg = new Blob([svgText], {
          type: "image/svg+xml;charset=utf-8"
        });
        
        // create a dom object for that image
        var url = domUrl.createObjectURL(svg);
        
        // create a new image to hold it the converted type
        var img = new Image;
        
        // when the image is loaded we can get it as base64 url
        img.onload = function() {
          // draw it to the canvas
          ctx.drawImage(this, margin, margin);
          
          // if it needs some styling, we need a new canvas
          if (fill) {
            var styled = document.createElement("canvas");
            styled.width = canvas.width;
            styled.height = canvas.height;
            var styledCtx = styled.getContext("2d");
            styledCtx.save();
            styledCtx.fillStyle = fill;   
            styledCtx.fillRect(0,0,canvas.width,canvas.height);
            styledCtx.strokeRect(0,0,canvas.width,canvas.height);
            styledCtx.restore();
            styledCtx.drawImage (canvas, 0,0);
            canvas = styled;
          }
          // we don't need the original any more
          domUrl.revokeObjectURL(url);
          // now we can resolve the promise, passing the base64 url
          resolve(canvas.toDataURL());

        };
        
        // load the image
        img.src = url;
        
      } catch (err) {
        reject('failed to convert svg to png ' + err);
      }
    });
  };
    
  ns.getGroup = function (groupName) {
    return Array.prototype.slice.apply(document.getElementsByName (groupName));  
  }
  
  ns.getOptions = function (selectElem) {
    var sel = ns.elem (selectElem);
    var options = Array.prototype.slice.call(sel.options || []);
    return options.map(function (d) {
      return d.value;
    });
  } 
  
  ns.changeOptions = function (selectElem , newOptions, selected) {
    sel = ns.elem (selectElem);
    sel.innerHTML = "";

    newOptions.forEach (function (d,i) {
      if (typeof d === "object") {
        ns.addElem (sel , "option" , d.text).value = d.value;
      }
      else {
        ns.addElem (sel , "option" , d).value = d;
      }
    });
    sel.value = selected;
    return ns;
  };
  
  ns.getChecked = function (groupName) {
    
    var filt = (document.getElementsByName (groupName) || []).filter(function(d) {
      return d.checked;
    });
    return filt.length ? filt[0] : null;
  }
  
  ns.elem = function(name) {
    if (typeof name === 'string') {
      return document.getElementById(name.replace(/^#/, ""));
    } else {
      return name;
    }
  };
  ns.addStyles = function(elem, styles) {
    if (styles) {
      styles.toString().split(";").forEach(function(d) {
        if (d) {
          var s = d.split(":");
          
          if (s.length !== 2) {
            throw "invalid style " + d;
          }
          elem.style[s[0]] = s[1];
        }
      });
    }
    return elem;
  };
  ns.addElem = function(parent, type, text, className, styles) {
    parent = ns.elem(parent);
    var elem = document.createElement(type);
    parent.appendChild(elem);
    elem.innerHTML = typeof text === typeof undefined ? '' : text;
    if (className) {
      elem.className += (" " + className);
    }
    return ns.addStyles(elem, styles);
    
  };
  
  ns.addClass = function(element, className) {
    element = ns.elem(element);
    if (!element.classList.contains(className)) {
      element.classList.add(className);
    }
    return element;
  };
    
  /**
  * apply a class to a div
  * @param {element} element
  * @param {boolean} addClass whether to remove or add
  * @param {string} [className] the class
  * @return {element} the div
  */
  ns.applyClass = function(element, addClass, className) {
    return ns.hide (element , addClass , className)
  };
  /**
  * apply a class to a div
  * @param {element} element
  * @param {boolean} addClass whether to remove or add
  * @param {string} [className] the class
  * @return {element} the div
  */
  ns.hide = function(element, addClass, className) {
    element = ns.elem(element);
    
    className = className || "mui--hide";
    // will only happen if polyfill not loaded..
    if (!element.classList.add) {
      throw 'classlist not supported';
    }
    var q = addClass ? ns.addClass(element, className) : element.classList.remove(className);
    return element;
  };
  
  /**
  * flip a div
  * @param {element} element
  * @param {string} [className] the class
  * @return {element} the div
  */
  ns.flip = function(element, className) {
    element = ns.elem(element);
    element.classList.toggle(className || "mui--hide");
    return element;
  };
  
  /**
  * is hidden
  * @param {element} element
  * @param {string} [className]
  * @return {boolean} is it hidden
  */
  ns.isHidden = function(element, className) {
    element = ns.elem(element);
    return element.classList.contains(className || "mui--hide");
  };
  
  /**
  * gets context of elem if text is preceded by # and the elem exists
  *@param {string} label the label or elem id to get
  *@return {string} the result
  */
  ns.fillLabel = function(label) {
    if (label && label.toString().slice(0, 1) === '#') {
      var elem = ns.elem(label);
      return elem ? elem.innerHTML : label;
    }
    return label;
    
  }
  return ns;
})(DomUtils || {});

