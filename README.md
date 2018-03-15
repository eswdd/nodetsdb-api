# nodetsdb-api
Library providing OpenTSDB API via Express to a given (seperate) backend.

# usage

```
var express = require('express');
var api = require('nodetsdb-api');

// init api with a backend
var backend = {};
api.backend(backend);

// install the app
var conf = {
    port: 4242,
    verbose: false,
    logRequests: false
};

var app = express();
api.install(app, conf);

// start the server
var server = app.listen(config.port, function() {
    var host = server.address().address
    var port = server.address().port

    console.log('NodeTSDB API running at http://%s:%s', host, port)
});
```