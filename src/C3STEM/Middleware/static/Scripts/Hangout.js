/*
* Copyright (c) 2011 Google Inc.
*
* Licensed under the Apache License, Version 2.0 (the "License"); you may not
* use this file except in compliance with the License. You may obtain a copy of
* the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
* WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
* License for the specific language governing permissions and limitations under
* the License.
*/

var forbiddenCharacters = /[^a-zA-Z!0-9_\- ]/;
function setText(element, text) {
  element.innerHTML = typeof text === 'string' ?
      text.replace(forbiddenCharacters, '') :
      '';
}

function updateMessageUi(state) {
  var msgElement = document.getElementById('message');

  setText(msgElement, 'in update Message UI');
}


// The functions triggered by the buttons on the Hangout App
function helloButtonClickX() {
	var mesg = document.getElementById('message');
  setText(msg, 'Hello Button Click');
}

// A function to be run at app initialization time which registers our callbacks
var msg = document.getElementById('message');
function initxx() {
  console.log('Init app.');

  var apiReady = function(eventObj) {
    if (eventObj.isApiReady) {
      console.log('API is ready');

			gapi.hangout.data.onMessageReceived.add(function(eventObj) {
        updateMessageUi(eventObj);
      });
			setText(msg, 'in init function');
      gapi.hangout.onApiReady.remove(apiReady);
    }
  };

  // This application is pretty simple, but use this special api ready state
  // event if you would like to any more complex app setup.
  gapi.hangout.onApiReady.add(apiReady);
}

//setText(msg, 'in body function');gadgets.util.registerOnLoadHandler(init);

$(function () {

    $.get( "/readVehicles", function( data ) {
      alert(data);
      alert( "Load in Hangout.js was performed." );
    });

  var postdata = {};

  jsonData = $.ajax({
    url: "/readVehicles",
    dataType: "json",
    async: false,
    data: postdata
  }).responseText;

  try {
    alert(jsonData);
    data = jQuery.parseJSON(jsonData);
  }
  catch (e) {
    
  }
}); 