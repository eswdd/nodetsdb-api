[![NPM version][npm-version-image]][npm-url]
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

# backend spec

```
/**
 * Get the uid meta data for the given named item.
 * @param type The item type, one of (metric, tagk or tagv)
 * @param name The item name
 * @returns {name:String, uid:String, created:Date} or undefined
 */
backend.uidMetaFromName = function(type, name);
```
```
/**
 * Get the uid meta data for the given uid.
 * @param type The item type, one of (metric, tagk or tagv)
 * @param name The item uid
 * @returns {name:String, uid:String, created:Date} or undefined
 */
backend.uidMetaFromUid = function(type, uid);
```
```
/**
 * Search for timeseries.
 * @param metric String
 * @param limit Number
 * @param useMeta Boolean
 * @returns Array of {
 *                     metric: String,
 *                     tags: { tagk1:tagv1, tagk2:tagv2, ... },
 *                     tsuid: String
 *                   }
backend.searchLookupImpl = function(metric, limit, useMeta);
```
```
/**
 * Loads time series data for the given query, applies pre-query filtering where possible
 * @param startTime DateTime
 * @param endTime DateTime
 * @param ms Boolean
 * @param downsample String
 * @param metric String
 * @param filters Array of {tagk:String,type:String,filter:[String],group_by:Boolean}
 * @returns Array of {
 *                     metric:String,
 *                     metric_uid:String,
 *                     tags: { tagk: { tagk:String, tagk_uid:String, tagv:String, tagv_uid:String} }
 *                     dps: [ [ timestamp:Number, value:Number ] ]
 *                   }
 */
backend.performBackendQueries = function(startTime, endTime, ms, downsampled, metric, filters);
```
```
/**
 * Query for annotations for a set of timeseries.
 * @param startTime DateTime
 * @param endTime DateTime
 * @param downsampleSeconds Number
 * @param ms Boolean
 * @param participatingTimeSeries Array of {
 *                                           metric:String,
 *                                           metric_uid:String,
 *                                           tags: { tagk: { tagk:String, tagk_uid:String, tagv:String, tagv_uid:String} }
 *                                           dps: [ [ timestamp:Number, value:Number ] ]
 *                                         }
 * @returns Array of {
 *                     tsuid:String,
 *                     description:String,
 *                     notes:String,
 *                     custom:Map,
 *                     startTime:Date,
 *                     endTime:Date
 *                   }
 */
backend.performAnnotationsQueries = function(startTime, endTime, downsampleSeconds, ms, participatingTimeSeries)
```
```
/**
 * Query for annotations for a set of timeseries.
 * @param startTime DateTime
 * @param endTime DateTime
 * @returns Array of {
 *                     tsuid:String,
 *                     description:String,
 *                     notes:String,
 *                     custom:Map,
 *                     startTime:Date,
 *                     endTime:Date
 *                   }
 */
backend.performGlobalAnnotationsQuery = function(startTime, endTime);
```
```
/**
 * Look for metrics starting with a given query.
 * @oaram query String (or undefined)
 * @returns Array of String
 */
backend.suggestMetrics = function(query);
```


[license-image]: http://img.shields.io/badge/license-GPL-blue.svg?style=flat
[license-url]: LICENSE

[npm-url]: https://npmjs.org/package/nodetsdb-api
[npm-version-image]: http://img.shields.io/npm/v/nodetsdb-api.svg?style=flat
[npm-downloads-image]: http://img.shields.io/npm/dm/nodetsdb-api.svg?style=flat

[travis-url]: http://travis-ci.org/eswdd/nodetsdb-api
[travis-image]: http://img.shields.io/travis/eswdd/nodetsdb-api/master.svg?style=flat

[coverage-url]: https://coveralls.io/r/eswdd/nodetsdb-api
[coverage-image]: https://coveralls.io/repos/github/eswdd/nodetsdb-api/badge.svg