var express = require('express');
var path = require('path');
var favicon = require('serve-favicon');
var logger = require('morgan');
var cookieParser = require('cookie-parser');
var bodyParser = require('body-parser');
var io = require('socket.io');
var http = require('http');
var ip = require('ip');

var routes = require('./routes/index');

// set up a server
var addr = ip.address();
var port = 3000;
var app = express();
var server = http.createServer(app);
server.listen(port, addr);

// set up socket.io
var socket = io.listen(server);
console.log("Server listening to " + addr + " " + port);

// set up event handlers for client
socket.on('connection', onConnect);

// get redis server ip
var redis_port = process.argv[2];
var redis_ip = process.argv[3];

// create redis client
var redis = require('redis');
var redisClient = redis.createClient(redis_port, redis_ip);

// set up event handlers for redis
redisClient.on('connect', redisOnConnnect);
redisClient.on('error', redisOnError);

// define handler functions for client
function onConnect(socket) {
	console.log("Client connected");
	socket.on('disconnect', onDisconnect);
	socket.on('query', search);
}

function onDisconnect(socket) {
	console.log("Client disconnected");
}

function search(data) {
	var interestAND = data.interestAND.toLowerCase();
	var interestOR = data.interestOR.toLowerCase();
	var ignore = data.ignore.toLowerCase();

	if(interestAND.length != 0)
		interestAND = interestAND.split(" ");
	if(interestOR.length != 0)
		interestOR = interestOR.split(" ");
	if(ignore.length != 0)
		ignore = ignore.split(" ");

	// debug print
	// console.log("\ninterestAND : " + interestAND);
	// console.log("interestOR : " + interestOR);
	// console.log("ignore : " + ignore + "\n");

	// start a multi command execution
	var multiInvIdx = redisClient.multi();

	// delete any old results
	multiInvIdx.del("QUERY_AND_RESULT");
	multiInvIdx.del("QUERY_OR_RESULT");
	multiInvIdx.del("QUERY_RESULT");

	// union all the OR interest keywords
	for(var i = 0; i < interestOR.length; i++) {
		multiInvIdx.sunionstore("QUERY_OR_RESULT", "QUERY_OR_RESULT", "RII_" + interestOR[i]);
	}

	// intersect all the AND interest keywords
	for(i = 0; i < interestAND.length; i++) {
		if(i == 0)
			multiInvIdx.sunionstore("QUERY_AND_RESULT", "QUERY_AND_RESULT", "RII_" + interestAND[i]);
		else
			multiInvIdx.sinterstore("QUERY_AND_RESULT", "QUERY_AND_RESULT", "RII_" + interestAND[i]);
	}

	if(interestAND.length == 0 || interestOR.length == 0 ) {
		// union the AND and OR terms
		multiInvIdx.sunionstore("QUERY_RESULT", "QUERY_AND_RESULT", "QUERY_OR_RESULT");
	} else {
		// intersect the AND and OR terms
		multiInvIdx.sinterstore("QUERY_RESULT", "QUERY_AND_RESULT", "QUERY_OR_RESULT");
	}

	// remove all the ignore keywords
	for(i = 0; i < ignore.length; i++) {
		multiInvIdx.sdiffstore("QUERY_RESULT", "QUERY_RESULT", "RII_" + ignore[i]);
	}

	// get the query results
	multiInvIdx.smembers("QUERY_RESULT");

	// delete any old results
	multiInvIdx.del("QUERY_AND_RESULT");
	multiInvIdx.del("QUERY_OR_RESULT");
	multiInvIdx.del("QUERY_RESULT");

	// start a multi command execution
	var multiScore = redisClient.multi();

	// execute the multistep execution
	multiInvIdx.exec(function(err1, replies) {
		var offset = interestAND.length + interestOR.length + ignore.length + 4;
		var results = replies[offset];

		// sort and send the results
		var resultObjs = [];

		// emit the count of results obtained
		socket.emit('count', {count : results.length});

		// build result objects
		for(i = 0; i < results.length; i++) {
			var wikiTitle = results[i].toString();
			
			var key = "RLC_" + wikiTitle;
			multiScore.hget(key, "count")
		}

		multiScore.exec(function(err2, resultScores) {
			for(var k = 0; k < resultScores.length; k++) {
				// get the score for each result link
				var score = parseInt(resultScores[k]);

				// push to list
				var wikiTitle = results[k].toString();
				resultObjs.push({score : score, title : wikiTitle});
			}

			// sort list
			resultObjs.sort(function(a, b) {
				return b.score - a.score;
			});

			// for each sorted list
			resultObjs.forEach(function (obj, index) {
				// emit min score and title
				socket.emit('result', {score : obj.score, title : obj.title});
			});
		});
	});
}

// define handler functions for redis
function redisOnConnnect() {
    console.log('Connected to redis server');
}

function redisOnError() {
    console.log('Error in redis');
}

// view engine setup
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'jade');

// uncomment after placing your favicon in /public
//app.use(favicon(__dirname + '/public/favicon.ico'));
app.use(logger('dev'));
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));
app.use(cookieParser());
app.use(express.static(path.join(__dirname, 'public')));

app.use('/', routes);

// catch 404 and forward to error handler
app.use(function(req, res, next) {
    var err = new Error('Not Found');
    err.status = 404;
    next(err);
});

// error handlers

// development error handler
// will print stacktrace
if (app.get('env') === 'development') {
    app.use(function(err, req, res, next) {
        res.status(err.status || 500);
        res.render('error', {
            message: err.message,
            error: err
        });
    });
}

// production error handler
// no stacktraces leaked to user
app.use(function(err, req, res, next) {
    res.status(err.status || 500);
    res.render('error', {
        message: err.message,
        error: {}
    });
});


module.exports = app;
