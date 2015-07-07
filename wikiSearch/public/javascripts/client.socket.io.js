var server_name = "http://127.0.0.1:3000/";
var socket = io.connect(server_name);
console.log('Client connected to server ' + server_name);

// set a handler for onclick event of "search" button
var button = document.getElementById("search");
button.onclick = handleSearch;

// set up a result event handler
socket.on('result', handleResult);
socket.on('count', handleCount);

// function to handle search query request
function handleSearch(e) {
	var _interestAND = $('#interestAND').val();
	var _interestOR = $('#interestOR').val();
	var _ignore = $('#ignore').val();

	// remove all previous search results
	var results = $("#results");
	results.empty();
	var count = $("#count");
	count.empty();

	// send new query to server
	socket.emit('query', {interestAND: _interestAND, interestOR : _interestOR, ignore: _ignore});
}

// function to handle search result
function handleResult(data) {
	var results = $("#results");

	// add the result to the list to present on webpage
	var link = "http://www.en.wikipedia.org/wiki/" + encodeURI(data.title);
	results.append("<li id='wiki_link'><a href='" + link + "' target='_blank'>" + data.title + " (" + data.score + ")" + "</a></li>");
}

// function to handle search result count
function handleCount(data) {
	var count = $("#count");
	count.text("Found " + data.count + " results");
}