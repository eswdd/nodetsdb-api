[![GPL License][license-image]][license-url]
[![Build Status][travis-image]][travis-url]
[![Coverage][coverage-image]][coverage-url]


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


[license-image]: http://img.shields.io/badge/license-GPL-blue.svg?style=flat
[license-url]: LICENSE

[travis-url]: http://travis-ci.org/eswdd/nodetsdb-api
[travis-image]: http://img.shields.io/travis/eswdd/nodetsdb-api/master.svg?style=flat

[coverage-url]: https://coveralls.io/r/eswdd/nodetsdb-api
[coverage-image]: https://coveralls.io/repos/github/eswdd/nodetsdb-api/badge.svg