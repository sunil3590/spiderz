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
	var interestAND = data.interestAND;
	var interestOR = data.interestOR;
	var ignore = data.ignore;

	if(interestAND.length != 0)
		interestAND = interestAND.split(" ");
	if(interestOR.length != 0)
		interestOR = interestOR.split(" ");
	if(ignore.length != 0)
		ignore = ignore.split(" ");

	console.log("\ninterestAND : " + interestAND);
	console.log("interestOR : " + interestOR);
	console.log("ignore : " + ignore + "\n");

	// start a multi command execution
	var multi = redisClient.multi();

	// delete any old results
	multi.del("QUERY_AND_RESULT");
	multi.del("QUERY_OR_RESULT");
	multi.del("QUERY_RESULT");

	// union all the OR interest keywords
	for(var i = 0; i < interestOR.length; i++) {
		multi.sunionstore("QUERY_OR_RESULT", "QUERY_OR_RESULT", "RII_" + interestOR[i]);
	}

	// intersect all the AND interest keywords
	for(i = 0; i < interestAND.length; i++) {
		if(i == 0)
			multi.sunionstore("QUERY_AND_RESULT", "QUERY_AND_RESULT", "RII_" + interestAND[i]);
		else
			multi.sinterstore("QUERY_AND_RESULT", "QUERY_AND_RESULT", "RII_" + interestAND[i]);
	}

	if(interestAND.length == 0 || interestOR.length == 0 ) {
		// union the AND and OR terms
		multi.sunionstore("QUERY_RESULT", "QUERY_AND_RESULT", "QUERY_OR_RESULT");
	} else {
		// intersect the AND and OR terms
		multi.sinterstore("QUERY_RESULT", "QUERY_AND_RESULT", "QUERY_OR_RESULT");
	}

	// remove all the ignore keywords
	for(i = 0; i < ignore.length; i++) {
		multi.sdiffstore("QUERY_RESULT", "QUERY_RESULT", "RII_" + ignore[i]);
	}

	// get the query results
	multi.smembers("QUERY_RESULT");

	// delete any old results
	multi.del("QUERY_AND_RESULT");
	multi.del("QUERY_OR_RESULT");
	multi.del("QUERY_RESULT");

	// start a multi command execution
	var multiScore = redisClient.multi();

	// execute the multistep execution
	multi.exec(function(err1, replies) {
		var offset = interestAND.length + interestOR.length + ignore.length + 4;
		var results = replies[offset];

		// sort and send the results
		var resultObjs = [];

		// emit the count of results obtained
		socket.emit('count', {count : results.length});

		// build result objects
		for(i = 0; i < results.length; i++) {
			var wikiTitle = results[i].toString();
			var min = 9007199254740992; // max value of int in javascript

			for(var j = 0; j < 8; j++) {
				var key = getKey(wikiTitle, j);
				multiScore.hget(key, "count")
			}
		}

		multiScore.exec(function(err2, resultScores) {
			for(var k = 0; k < results.length; k++) {
				// handle in batches of 8
				for(var row = 0; row < 8; row++) {
					// find min
					var sc = parseInt(resultScores[8*k + row])
					if(sc < min)
						min = sc;
				}

				// push to list
				var wikiTitle = results[k].toString();
				resultObjs.push({score : min, title : wikiTitle});
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
    console.log('error in redis');
}

// this is a hack. server and client both need to follow 
// same seed and size of count min sketch

// hash the title and return key to query redis count min
function getKey(title, row) {
	// seeds to build keys for titls
	var seed = [4962, 274836, 7527385, 321459, 9864761, 649, 176924826,
	57549862];

	var size = 1024 * 1024 * 32;

	var hash = murmur2mod(title, seed[row]) % size;

	var key = "RCM_" + row.toString() + "_" + hash.toString();

	//console.log(title + " - " + key);

	return key;
}

// https://github.com/perezd/node-murmurhash/blob/master/murmurhash.js
function murmur2mod(str, seed) {
    var
      l = str.length,
      h = seed ^ l,
      i = 0,
      k;

    var mul = 1;

    while (l >= 4) {
		k =
			((str.charCodeAt(i) & 0xff)) |
			((str.charCodeAt(++i) & 0xff) << 8) |
			((str.charCodeAt(++i) & 0xff) << 16) |
			((str.charCodeAt(++i) & 0xff) << 24);

		k = (((k & 0xffff) * mul) + ((((k >>> 16) * mul) & 0xffff) << 16));
		k ^= k >>> 24;
		k = (((k & 0xffff) * mul) + ((((k >>> 16) * mul) & 0xffff) << 16));

		h = (((h & 0xffff) * mul) + ((((h >>> 16) * mul) & 0xffff) << 16)) ^ k;

		l -= 4;
		++i;
    }

	switch (l) {
		case 3: h ^= (str.charCodeAt(i + 2) & 0xff) << 16;
		case 2: h ^= (str.charCodeAt(i + 1) & 0xff) << 8;
		case 1: h ^= (str.charCodeAt(i) & 0xff);
		        h = (((h & 0xffff) * mul) + ((((h >>> 16) * mul) & 0xffff) << 16));
	}

	h ^= h >>> 13;
	h = (((h & 0xffff) * mul) + ((((h >>> 16) * mul) & 0xffff) << 16));
	h ^= h >>> 15;
	
	h = h >>> 0;

	// take only 31 bits. this is what the server does
	h31 = h & 0x7fffffff;
	
	return h31;
  };

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
