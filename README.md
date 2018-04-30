[![NPM version][npm-version-image]][npm-url]
[![GPL License][license-image]][license-url]
[![Build Status][travis-image]][travis-url]
[![Coverage][coverage-image]][coverage-url]


# nodetsdb-api
Library providing OpenTSDB API via Express to a given (seperate) backend.

Current implemented endpoints:

 * /api/aggregators - GET
 * /api/aggregators - POST
 * /api/annotation - DELETE
 * /api/annotation - POST
 * /api/annotation/bulk - POST
 * /api/config - GET
 * /api/put - POST
 * /api/query - GET
 * /api/query/gexp - GET
 * /api/search/lookup - GET
 * /api/search/lookup - POST
 * /api/suggest - GET
 * /api/uid/uidmeta - GET
 * /api/version - GET
 * /api/version - POST

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
 * Get the uid meta data for the given uid.
 * @param type The item type, one of (metric, tagk or tagv)
 * @param name The item uid
 * @param callback Callback function, arguments (data, err), where data is {name:String, uid:String, created:Date} or undefined
 */
backend.uidMetaFromUid = function(type, uid, callback);
```
```
/**
 * Search for timeseries.
 * @param metric String
 * @param limit Number
 * @param useMeta Boolean
 * @param callback Callback function, arguments (data, err), where data is Array of {
 *                     metric: String,
 *                     tags: { tagk1:tagv1, tagk2:tagv2, ... },
 *                     tsuid: String
 *                   }
backend.searchLookupImpl = function(metric, limit, useMeta, callback);
```
```
/**
 * Loads time series data for the given query, applies pre-query filtering where possible
 * @param startTime DateTime
 * @param endTime DateTime
 * @param downsample String
 * @param metric String
 * @param filters Array of {tagk:String,type:String,filter:[String],group_by:Boolean}
 * @param callback Callback function, arguments (data, err), where data is Array of {
 *                     metric:String,
 *                     metric_uid:String,
 *                     tsuid:String,
 *                     tags: { tagk: { tagk:String, tagk_uid:String, tagv:String, tagv_uid:String} }
 *                     dps: [ [ timestamp:Number, value:Number ] ]
 *                   }
 */
backend.performBackendQueries = function(startTime, endTime, downsampled, metric, filters, callback);
```
```
/**
 * Query for annotations for a set of timeseries.
 * @param startTime DateTime
 * @param endTime DateTime
 * @param downsampleSeconds Number
 * @param participatingTimeSeries Array of {
 *                                           metric:String,
 *                                           metric_uid:String,
 *                                           tags: { tagk: { tagk:String, tagk_uid:String, tagv:String, tagv_uid:String} }
 *                                           dps: [ [ timestamp:Number, value:Number ] ]
 *                                         }
 * @param callback Callback function, arguments (data, err), where data is Array of {
 *                     tsuid:String,
 *                     description:String,
 *                     notes:String,
 *                     custom:Map,
 *                     startTime:Date,
 *                     endTime:Date
 *                   }
 */
backend.performAnnotationsQueries = function(startTime, endTime, downsampleSeconds, participatingTimeSeries, callback)
```
```
/**
 * Query for annotations for a set of timeseries.
 * @param startTime DateTime
 * @param endTime DateTime
 * @param callback Callback function, arguments (data, err), where data is Array of {
 *                     tsuid:String,
 *                     description:String,
 *                     notes:String,
 *                     custom:Map,
 *                     startTime:Date,
 *                     endTime:Date
 *                   }
 */
backend.performGlobalAnnotationsQuery = function(startTime, endTime, callback);
```
```
/**
 * Look for metrics starting with a given query.
 * @param query String (or undefined)
 * @param max Integer (or undefined)
 * @param callback Callback function, arguments (data, err), where data is Array of String
 */
backend.suggestMetrics = function(query, max, callback);
```
```
/**
 * Look for tag keys starting with a given query.
 * @param query String (or undefined)
 * @param max Integer (or undefined)
 * @param callback Callback function, arguments (data, err), where data is Array of String
 */
backend.suggestTagKeys = function(query, max, callback);
```
```
/**
 * Look for tag values starting with a given query.
 * @param query String (or undefined)
 * @param max Integer (or undefined)
 * @param callback Callback function, arguments (data, err), where data is Array of String
 */
backend.suggestTagValues = function(query, max, callback);
```
```
/**
 * Store datapoints.
 * @param points Array of {
 *                          metric:String,
 *                          timestamp:Number,
 *                          value:Number|String,
 *                          tags: { tagk1:tagv1, tagk2:tagv2, ... }
 *                        }
 * @param callback Callback function, arguments (data, err), where data is Array of String (same length as points parameter, value if error at each index, undefined if success)
 */
backend.storePoints = function(points, callback);
```
```
/**
 * Store annotations
 * @param annotations Array of {
 *                     tsuid:String,
 *                     description:String,
 *                     notes:String,
 *                     custom:Map,
 *                     startTime:Date,
 *                     endTime:Date
 *                   }
 * @param callback Callback function, arguments (annotations, err)
 */
backend.storeAnnotations = function(annotations, callback);
```
```
/**
 * Delete annotation
 * @param annotation {
 *                     tsuid:String,
 *                     description:String,
 *                     notes:String,
 *                     custom:Map,
 *                     startTime:Date,
 *                     endTime:Date
 *                   }
 * @param callback Callback function, arguments (null, err)
 */
backend.deleteAnnotation = function(annotation, callback);
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