var express = require('express');
var path = require('path');
var favicon = require('serve-favicon');
var logger = require('morgan');
var cookieParser = require('cookie-parser');
var bodyParser = require('body-parser');
var io = require('socket.io');
var http = require('http');

var routes = require('./routes/index');
var users = require('./routes/users');

// set up a server and socket.io
var app = express();
var server = http.createServer(app);
server.listen(3000);
var socket = io.listen(server);
console.log("Server listening to http://127.0.0.1:3000/");

// set up event handlers for client
socket.on('connection', onConnect);

// create redis client
var redis = require('redis');
var redisClient = redis.createClient();

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
	var interest = data.interest;
	var ignore = data.ignore;

	interest = interest.split(" ");
	ignore = ignore.split(" ");

	// start a multi command execution
	var multi = redisClient.multi();

	// delete any old results
	multi.del("QUERY_result");

	// union all the interest keywords
	for(var i = 0; i < interest.length; i++) {
		multi.sunionstore("QUERY_result", "QUERY_result", "RII_" + interest[i]);
	}

	// remove all the ignore keywords
	for(i = 0; i < ignore.length; i++) {
		multi.sdiffstore("QUERY_result", "QUERY_result", "RII_" + ignore[i]);
	}

	// get the query results
	multi.smembers("QUERY_result");

	// delete any old results
	multi.del("QUERY_result");

	// execute the multistep execution
	multi.exec(function(err1, replies) {
		var results = replies[interest.length + ignore.length + 1];

		// sort and send the results
		var resultsObjs = [];

		// emit the count of results obtained
		socket.emit('count', {count : results.length});

		// build result objects
		results.forEach(function (reply, index) {
			var wikiTitle = reply.toString();

			redisClient.hget("RCM_" + wikiTitle, "count", function(err2, count) {
				resultsObjs.push({score : count, title : wikiTitle});

				if(index == results.length - 1) {
					// sort result objects
					resultsObjs.sort(function(a, b) {
						return b.score - a.score;
					});

					// emit each result title
					resultsObjs.forEach(function (obj, index) {
						socket.emit('result', {score : obj.score, title : obj.title});
					});
				}
			});
		});
	});
}

// define handler functions for redis
function redisOnConnnect() {
    console.log('Connected to redis server');
}

function redisOnError() {
    console.log('error in redis');
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
app.use('/users', users);

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
