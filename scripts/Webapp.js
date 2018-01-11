'use strict';
function doGet(e) {

  var ui = HtmlService.createTemplateFromFile('index.html')
      .evaluate()
      .setSandboxMode(HtmlService.SandboxMode.IFRAME)
      .setTitle("Apps script orchestration");

  return ui;
}
