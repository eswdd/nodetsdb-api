var express = require('express');
var cors = require('cors');
var bodyParser = require('body-parser');
var router = express.Router();
router.use(bodyParser.json());
var moment = require('moment');

var backend = undefined;
// public interface:
//   backend.uidMetaFromUid(type, uid, callback);
//   backend.searchLookupImpl(metric, limit, useMeta, callback);
//   backend.performBackendQueries(startTime, endTime, downsampled, metric, filters, callback);
//   backend.performAnnotationsQueries(startTime, endTime, downsampleSeconds, participatingTimeSeries, callback)
//   backend.performGlobalAnnotationsQuery(startTime, endTime, callback);
//   backend.suggestMetrics(query, callback)
//   backend.storePoints(points, callback)

var handleErr = function(res, err) {
    if (err) {
        res.status(err.code ? err.code : 500).send(err);
        return true;
    }
    return false;
}


var aggregatorsImpl = function(req, res) {
    // if add more here then add support in query implementation
    res.json(["avg","sum","min","max","count"]);
};

var putImpl = function(req, res) {
    if (!backend.storePoints) {
        res.json(null, {code:501,message:"Storing points not supported by this backend"});
        return;
    }
    var queryParams = req.query;
    var detailed = "detailed" in queryParams;
    var summary = "summary" in queryParams;
    var points = req.body;
    if (!(points instanceof Array)) {
        points = [req.body];
    }

    //console.log("backend.storePoints("+JSON.stringify(points)+")");
    backend.storePoints(points, function(responses) {
        if (responses === undefined) {
            res.send("Error processing put");
            res.status(500);
            return;
        }

        var successCount = 0;
        var failCount = 0;
        for (var i=0; i<points.length; i++) {
            if (responses[i] === undefined) {
                successCount++;
            }
            else {
                failCount++;
            }
        }

        if (failCount === 0) {
            if (detailed || summary) {
                res.status(200);
            }
            else {
                res.status(204);
            }
        }
        else {
            res.status(400)
        }

        var response = {
            "failed": failCount,
            "success": successCount
        };
        if (detailed) {
            var errors = [];
            for (var i=0; i<points.length; i++) {
                var msg = responses[i];
                if (msg !== undefined) {
                    errors.push({
                        "datapoint": points[i],
                        "error": msg
                    });
                }
            }
            response["errors"] = errors;
        }
        if (summary || detailed) {
            res.json(response)
        }
        else {
            res.send(204);
        }
    });
};

var annotationPostImpl = function(req, res) {
    if (!backend.storeAnnotations) {
        handleErr({code:501,message:"Storing annotations not supported by this backend"});
        return;
    }
    var annotation = req.body;
    backend.storeAnnotations([annotation], function(errs) {
        if (!handleErr(res, errs[0])) {
            res.json(annotation);
        }
    });
};

var annotationDeleteImpl = function(req, res) {
    if (!backend.deleteAnnotation) {
        handleErr({code:501,message:"Deleting annotations not supported by this backend"});
        return;
    }
    var annotation = req.body;
    backend.deleteAnnotation(annotation, function(result, err) {
        if (!handleErr(res, err)) {
            res.status(204);
        }
    });
};

var annotationBulkPostImpl = function(req, res) {
    if (!backend.storeAnnotations) {
        handleErr({code:501,message:"Bulk storing annotations not supported by this backend"});
        return;
    }
    backend.storeAnnotations(req.body, function(errs) {
        var errCount = 0;
        var count = req.body.length;
        for (var i=0; i<count; i++) {
            if (errs[i] !== undefined) {
                errCount++;
            }
        }
        if (errCount != 0) {
            console.log("Got "+errCount+" errors whilst writing bulk annotations")
        }
        // todo: what do i do?
        res.json(req.body);
    });
};

var parseSearchLookupQuery = function(query, callback) {
    if (query == null) {
        callback(null, "You must specify a query");
        return;
    }
    if (query === "" || query === "{}") {
        callback(null, "You must specify at least one metric, tagk or tagv in your query");
        return;
    }

    var ret = {
        metric: null,
        tags: []
    };
    var firstBrace = query.indexOf("{");
    if (firstBrace === -1) {
        ret.metric = query;
        callback(ret, null);
        return;
    }

    var secondBrace = query.indexOf("}", firstBrace+1);
    if (secondBrace === -1) {
        callback(null, "Can't parse query: '"+query+"'");
        return;
    }
    if (query.indexOf("{", secondBrace+1) !== -1) {
        callback(null, "Can't parse query: '"+query+"'");
        return;
    }

    if (firstBrace > 0) {
        ret.metric = query.substring(0, firstBrace);
    }
    ret.tags = [];
    var tagStrings = query.substring(firstBrace+1, secondBrace).split(",");
    for (var t=0; t<tagStrings.length; t++) {
        if (tagStrings[t].indexOf("=") === -1) {
            callback(null, "Can't parse query: '"+query+"'");
            return;
        }
        var tagArray = tagStrings[t].split("=");
        ret.tags.push({key:tagArray[0], value: tagArray[1]});
    }

    callback(ret, null);
};

var searchLookupImpl = function(query, limit, useMeta, res) {
    if (limit != null) {
        limit = parseInt(limit);
    }
    if (!backend.searchLookupImpl) {
        handleErr({code:501,message:"Searching not supported by this backend"});
        return;
    }
    parseSearchLookupQuery(query, function(parsedQuery, err) {
        if (!handleErr(res, err)) {
            backend.searchLookupImpl(parsedQuery, limit, useMeta, function (results, err) {
                if (!handleErr(res, err)) {
                    var ret = {
                        "type": "LOOKUP",
                        "metric": query,
                        "limit": limit,
                        "time": 1,
                        "results": results,
                        "startIndex": 0,
                        "totalResults": results.length
                    };
                    res.json(ret);
                }
            });
        }
    });
}

var searchLookupPost = function(req, res) {
    searchLookupImpl(req.body.metric, req.body.limit, req.body.useMeta, res);
};

var searchLookupGet = function(req, res) {
    var queryParams = req.query;
    searchLookupImpl(queryParams["m"], queryParams["limit"], queryParams["use_meta"], res);
};

var uidMetaGet = function(req, res) {
    if (!backend.uidMetaFromUid) {
        handleErr({code:501,message:"Uid metadata not supported by this backend"});
        return;
    }
    var queryParams = req.query;

    backend.uidMetaFromUid(queryParams["type"], queryParams["uid"], function (meta, err) {
        if (!handleErr(res, err)) {
            if (meta != null) {
                res.json({
                    uid: meta.uid,
                    name: meta.name,
                    created: meta.created,
                    type: queryParams["type"].toUpperCase()
                })
            }
            else {
                res.status(404).json({
                    code: 404,
                    message: queryParams["type"] + " with uid " + queryParams["uid"] + " not found"
                });
            }
        }
    });

};

var toDateTime = function(tsdbTime) {
    // force to string, needed for POST requests
    if (typeof(tsdbTime)==="number") {
        tsdbTime = "" + tsdbTime;
    }
    if (tsdbTime.indexOf("ago")<0) {
        if (tsdbTime.indexOf("/") >= 0) {
            return moment(tsdbTime, "YYYY/MM/DD HH:mm:ss").toDate();
        }
        return new Date(tsdbTime > 10000000000 ? tsdbTime : tsdbTime * 1000);
    }

    if (tsdbTime == null || tsdbTime === "") {
        return new Date();
    }

    tsdbTime = tsdbTime.split("-")[0];
    var numberComponent = tsdbTime.match(/^[0-9]+/);
    var stringComponent = tsdbTime.match(/[a-zA-Z]+$/);
    if (numberComponent.length === 1 && stringComponent.length === 1) {
        return moment().subtract(numberComponent[0], stringComponent[0]).toDate();
    }
    return new Date();
};

var avg = function(arr, startIndex, endIndex, itemFunction) {
    if (startIndex === undefined) {
        startIndex = 0;
    }
    if (endIndex === undefined) {
        endIndex = arr.length-1;
    }
    if (itemFunction === undefined) {
        itemFunction = function(a) {return a;};
    }

    return sum(arr, startIndex, endIndex, itemFunction)/count(arr, startIndex, endIndex, itemFunction);
};

var sum = function(arr, startIndex, endIndex, itemFunction) {
    if (startIndex === undefined) {
        startIndex = 0;
    }
    if (endIndex === undefined) {
        endIndex = arr.length-1;
    }
    if (itemFunction === undefined) {
        itemFunction = function(a) {return a;};
    }

    var ret = 0;
    for (var i=startIndex; i<=endIndex; i++) {
        ret += itemFunction(arr[i]);
    }
    return ret;
};

var count = function(arr, startIndex, endIndex /*, itemFunction */) {
    if (startIndex === undefined) {
        startIndex = 0;
    }
    if (endIndex === undefined) {
        endIndex = arr.length-1;
    }

    return (endIndex+1)-startIndex;
};

var min = function(arr, startIndex, endIndex, itemFunction) {
    if (startIndex === undefined) {
        startIndex = 0;
    }
    if (endIndex === undefined) {
        endIndex = arr.length-1;
    }
    if (itemFunction === undefined) {
        itemFunction = function(a) {return a;};
    }

    var ret = null;
    for (var i=startIndex; i<=endIndex; i++) {
        if (!ret) {
            ret = itemFunction(arr[i]);
        }
        else {
            ret = Math.min(itemFunction(arr[i]), ret);
        }
    }
    return ret;
};

var max = function(arr, startIndex, endIndex, itemFunction) {
    if (startIndex === undefined) {
        startIndex = 0;
    }
    if (endIndex === undefined) {
        endIndex = arr.length-1;
    }
    if (itemFunction === undefined) {
        itemFunction = function(a) {return a;};
    }

    var ret = null;
    for (var i=startIndex; i<=endIndex; i++) {
        if (!ret) {
            ret = itemFunction(arr[i]);
        }
        else {
            ret = Math.max(itemFunction(arr[i]), ret);
        }
    }
    return ret;
};


var functions = {
    "avg": avg,
    "sum": sum,
    "count": count,
    "min": min,
    "max": max
};


/**
 * Strips out time series which don't match the filters (for those filters which can only be applied post-query)
 * @param rawTimeSeries Array of {
 *                        metric:String,
  *                       metric_uid:String,
 *                        tags: { tagk: { tagk:String, tagk_uid:String, tagv:String, tagv_uid:String} }
 *                        dps: [ [ timestamp:Number, value:Number ] ]
 *                      }
 * @param filters Array of {tagk:String,type:String,filter:[String],group_by:Boolean}
 */
var postBackendFiltering = function(rawTimeSeries, filters) {
    // apply 
    for (var f=0; f<filters.length; f++) {
        var filter = filters[f];
        var fn = null;
        var ignoreCase = false;
        var negate = false;
        if (filter.type === "literal_or" || filter.type === "iliteral_or"
            || filter.type === "not_literal_or" || filter.type === "not_iliteral_or") {
            ignoreCase = filter.type.indexOf("iliteral") >= 0;
            negate = filter.type.indexOf("not_") === 0;
            if (ignoreCase) {
                for (var m=0; m<filter.filter.length; m++) {
                    filter.filter[m] = filter.filter[m].toLowerCase();
                }
            }
            fn = function(candidateValue) {
                var v = ignoreCase ? candidateValue.toLowerCase() : candidateValue;
                return filter.filter.indexOf(v) >= 0;
            };
        }
        if (filter.type === "wildcard" || filter.type === "iwildcard" || filter.type === "regexp") {
            ignoreCase = filter.type === "iwildcard";
            var matchAll = (filter.type.indexOf("wildcard") >= 0 && filter.filter === "*")
                        || (filter.type === "regexp" && filter.filter === ".*");
            if (matchAll) {
                fn = function(candidateValue) { return true; }
            }
            else {
                var regexp = filter.filter;
                if (filter.type.indexOf("wildcard") >= 0) {
                    regexp = regexp.split(".").join("\\\\.");
                    regexp = regexp.split("*").join(".*");
                    regexp = regexp.split("\\\\..*").join("\\\\.");
                }
                if (ignoreCase) {
                    regexp = regexp.toLowerCase();
                }
                fn = function(candidateValue) {
                    var v = ignoreCase ? candidateValue.toLowerCase() : candidateValue;
                    try {
                        return v.match(new RegExp(regexp)) != null;
                    }
                    catch (regexpError) {
                        // typical user error
                        if (regexp !== "*") {
                            console.log("regexp("+regexp+") caused an error: "+regexpError);
                        }
                        return false;
                    }
                };
            }
        }
        if (fn != null) {
            for (var t=rawTimeSeries.length - 1; t>=0; t--) {
                // time series doesn't have a tag we're querying
                if (!rawTimeSeries[t].tags.hasOwnProperty(filter.tagk)) {
                    rawTimeSeries.splice(t, 1);
                    continue;
                }
                var tagValue = rawTimeSeries[t].tags[filter.tagk].tagv;
                if (!fn(tagValue)) {
                    rawTimeSeries.splice(t, 1);
                    // noinspection UnnecessaryContinueJS
                    continue;
                }
            }
        }
    }   
};

/**
 * Construct the unique sets of tags found in the results which match the given filters.
 * @param rawTimeSeries Array of {
 *                        metric:String, 
 *                        metric_uid:String,
 *                        tags: { tagk: { tagk:String, tagk_uid:String, tagv:String, tagv_uid:String} }
 *                        dps: [ [ timestamp:Number, value:Number ] ]
 *                      }
 * @param filters Array of {tagk:String,type:String,filter:[String],group_by:Boolean}
 * @returns Array of { tagk1:tagv1, tagk2:tagv2, ... }
 */
    
var constructUniqueTagSetsFromRawResults = function(rawTimeSeries, filters) {
    var ret = [];
    var tagsIncluded = {};
    for (var f=0; f<filters.length; f++) {
        var filter = filters[f];
        if (filter.group_by) {
            tagsIncluded[filter.tagk] = filter.tagk;
        }
    }
//    console.log("tagsIncluded = "+JSON.stringify(tagsIncluded));
    
    // so now we want to remove any kv pairs where they're not included
    var sets = {};
    for (var t=0; t<rawTimeSeries.length; t++) {
        var ts = rawTimeSeries[t];
        var kvArray = [];
        var tagSet = {};
        for (var tagk in ts.tags) {
            if (ts.tags.hasOwnProperty(tagk) && tagsIncluded.hasOwnProperty(tagk)) {
                kvArray.push(tagk+":"+ts.tags[tagk].tagv);
                tagSet[tagk] = ts.tags[tagk].tagv;
            }
        }
        kvArray.sort();
        sets[JSON.stringify(kvArray)] = tagSet;

//        console.log("added to sets: "+JSON.stringify(kvArray)+" = "+JSON.stringify(tagSet));
    }
    for (var uniqueKey in sets) {
        if (sets.hasOwnProperty(uniqueKey)) {
            ret.push(sets[uniqueKey]);
        }
    }
    
    if (ret.length === 0) {
        ret.push({});
    }
    return ret;
};

/**
 * Get the set of data series that match this tagset.
 * @param rawTimeSeries Array of {
 *                        metric:String, 
 *                        metric_uid:String,
 *                        tags: { tagk: { tagk:String, tagk_uid:String, tagv:String, tagv_uid:String} }
 *                        dps: [ [ timestamp:Number, value:Number ] ]
 *                      }
 * @param tagset { tagk1:tagv1, tagk2:tagv2, ... }
 * @returns Array of {
 *                     metric:String, 
 *                     tags: { tagk: { tagk:String, tagk_uid:String, tagv:String, tagv_uid:String} }
 *                     dps: [ [ timestamp:Number, value:Number ] ]
 *                   }
 */
var rawTimeSeriesForTagSet = function(rawTimeSeries, tagset) {
    var ret = [];
    for (var t=0; t<rawTimeSeries.length; t++) {
        var ts = rawTimeSeries[t];
        var exclude = false;
        for (var tagk in tagset) {
            if (tagset.hasOwnProperty(tagk)) {
                if (!ts.tags.hasOwnProperty(tagk)) {
                    exclude = true;
                }
                else if (ts.tags[tagk].tagv != tagset[tagk]) {
                    exclude = true;
                }   
                else {
//                    console.log("Tag "+tagk+"("+tagset[tagk]+") passed as matches ts: "+ts.tags[tagk].tagv)
                }
            }
        }
        if (!exclude) {
            ret.push(ts);
        }
    }
    return ret;
};

var downsampleSingleTimeSeriesPoints = function(dps, startTime, endTime, downsampleIncrement, downsampleFunction, fillInPolicy) {
    var ret = [];
    var lastIndexInclusive = -1;
    for (var t=startTime; t<=endTime && lastIndexInclusive<dps.length-1; t+=downsampleIncrement) {
        var periodStart = t;
        var periodEnd = t+downsampleIncrement;
        var nextTimestamp = dps[lastIndexInclusive+1][0];
        if (periodStart <= nextTimestamp && nextTimestamp < periodEnd) {
            var startIndex = lastIndexInclusive+1;
            var endIndex = startIndex;
            while (endIndex < dps.length-1 && dps[endIndex+1][0] < periodEnd) {
                endIndex++;
            }

            var value = functions[downsampleFunction](dps, startIndex, endIndex, function(item) { return item[1]; });
            ret.push([t, value]);

            lastIndexInclusive = endIndex;
        }
        else {
            // now we need to use the fillInPolicy
            switch (fillInPolicy) {
                case "nan":
                    ret.push([t, NaN]);
                    break;
                case "null":
                    ret.push([t, null]);
                    break;
                case "zero":
                    ret.push([t, 0]);
                    break;
                case "none":
                default:
                    break;
            }
        }
    }
    return ret;
};

var combineTimeSeries = function(participatingTimeSeries, tagset, aggregateTags, startTime, endTime, metric, aggregator, ms, rate, arrays, showTsuids) {

    var tsuids = [];
    for (var p=0; p<participatingTimeSeries.length; p++) {
        tsuids.push(participatingTimeSeries[p].tsuid);
        if (config.verbose) {
            console.log("data = "+JSON.stringify(participatingTimeSeries[p].dps));
        }
    }

    /*
    Order of query operations (* are done in here):
        Filtering
        Grouping
        Downsampling
        Interpolation *
        Aggregation *
        Rate Conversion *
        Functions
        Expressions
     */

    // now combine data as appropriate
    var combinedDps = arrays ? [] : {};
    var indices = new Array(participatingTimeSeries.length);
    for (var i=0; i<indices.length; i++) {
        indices[i] = 0;
    }
    if (config.verbose) {
        console.log("    combining "+indices.length+" participating time series");
    }

    // first pass finds us the times of all points
    var seenTimestamps = {};
    var allTimestamps = [];
    for (var p=0; p<participatingTimeSeries.length; p++) {
        for (var d=0; d<participatingTimeSeries[p].dps.length; d++) {
            var t = participatingTimeSeries[p].dps[d][0];
            if (!seenTimestamps.hasOwnProperty(t)) {
                seenTimestamps[t] = t;
                allTimestamps.push(t);
            }
        }
    }
    allTimestamps.sort();

    var previousT = undefined;
    var previousVal = undefined;
    for (var it=0; it<allTimestamps.length; it++) {
        var t = allTimestamps[it];
        if (config.verbose) {
            console.log("     t = "+t);
        }
        var points = [];
        for (var i=0; i<indices.length; i++) {
            // each index should point to the lowest point which has a timestamp greater than t
            while (indices[i]<participatingTimeSeries[i].dps.length && participatingTimeSeries[i].dps[indices[i]][0]<t) {
                indices[i]++;
            }
            if (indices[i]<participatingTimeSeries[i].dps.length) {
                if (participatingTimeSeries[i].dps[indices[i]][0]===t) {
                    points.push(participatingTimeSeries[i].dps[indices[i]][1]);
                }
                else { // next dp time is greater than time desired
                    // can't interpolate from before beginning
                    if (indices[i]>0) {
                        var gapSizeTime = participatingTimeSeries[i].dps[indices[i]][0] - participatingTimeSeries[i].dps[indices[i]-1][0];
                        var gapDiff = participatingTimeSeries[i].dps[indices[i]][1] - participatingTimeSeries[i].dps[indices[i]-1][1];

                        var datumToNow = t - participatingTimeSeries[i].dps[indices[i]-1][0];
                        var datumToNowRatio = datumToNow / gapSizeTime;

                        var gapDiffMultRatio = datumToNowRatio * gapDiff;
                        var newVal = participatingTimeSeries[i].dps[indices[i]-1][1] + gapDiffMultRatio;
                        points.push(newVal);
                    }

                }
            }
        }
        if (config.verbose) {
            console.log("      For time "+t+", partipating points = "+JSON.stringify(points));
        }

        // now we have our data points, combine them:
        if (points.length > 0) {
            if (functions.hasOwnProperty(aggregator)) {
                val = functions[aggregator](points);
            }
            else {
                throw "unrecognized agg: "+aggregator;
            }

            if (!ms) {
                t = Math.round(t/1000);
            }

            if (rate) {
                var valDiff = undefined;
                if (previousVal !== undefined) {
                    valDiff = val - previousVal;
                }
                previousVal = val;
                if (previousT !== undefined) {
                    var timeDiff = t - previousT;
                    if (ms) {
                        timeDiff /= 1000;
                    }
                    val = valDiff / timeDiff;
                }
                else {
                    val = undefined;
                }
            }

            if (val !== undefined) {
                if (arrays) {
                    combinedDps.push([t, val]);
                }
                else {
                    combinedDps[t] = val;
                }
            }
            previousT = t;
        }
    }

    var toPush = {
        "metric": metric,
        "tags": tagset,
        "aggregatedTags": aggregateTags,
        "dps": combinedDps
    };

    if (showTsuids) {
        toPush.tsuids = tsuids;
    }

    return toPush;
}

var performSingleMetricQuery = function(startTime, endTime, m, arrays, ms, showQuery, annotations, globalAnnotations, globalAnnotationsArray, showTsuids, callback) {
    var colonSplit = m.split(":");
    var aggregator = colonSplit[0];
    var rate = false;
    var downsampled = false;
    if (colonSplit[1].indexOf("rate") === 0) {
        rate = true;
        // todo: consider supporting counters?
        if (colonSplit.length === 4) {
            downsampled = colonSplit[2];
        }
    }
    else {
        if (colonSplit.length === 3) {
            downsampled = colonSplit[1];
        }
    }
    var metricAndTags = colonSplit[colonSplit.length-1];
    var metric = metricAndTags;
    var filters = [];
    var openCurly = metricAndTags.indexOf("{");
    var closeCurly = metricAndTags.indexOf("}");
    if (openCurly >= 0) {
        metric = metricAndTags.substring(0, openCurly);
        var tagString = metricAndTags.substring(openCurly+1);
        tagString = tagString.substring(0, tagString.length-1);

        // some idiot specified the tag spec as {}
        if (tagString !== "") {
            var tagArray = tagString.split(",");
            for (var t=0; t<tagArray.length; t++) {
                var kv = tagArray[t].split("=");
                var tagk = kv[0];
                if (kv[1].indexOf("*")>=0) {
                    filters.push({tagk:tagk,type:"wildcard",filter:[kv[0]],group_by:true});
                }
                else if (kv[1].indexOf("|")>=0) {
                    filters.push({tagk:tagk,type:"literal_or",filter:kv[1].split("|"),group_by:true});
                }
                else {
                    filters.push({tagk:tagk,type:"literal_or",filter:[kv[1]],group_by:true});
                }
            }
        }
    }
    var query = {};
    if (showQuery) {
        query.aggregator = aggregator;
        query.metric = metric;
        query.tsuids = null;
        query.downsample = downsampled ? downsampled : null;
        query.rate = rate;
        query.explicitTags = false;
        query.rateOptions = null;
        query.tags = {};
        query.filters = []; // todo
        // for (var t=0; t<tags.length; t++) {
        //     query.tags[tags[t].tagk] = tags[t].tagv;
        //     var isWildcard = tags[t].tagv.indexOf("*") >= 0;
        //     query.filters.push({tagk:tags[t].tagk,type:(isWildcard?"wildcard":"literal_or"),filter:tags[t].tagv,group_by:true});
        // }
    }

    if (config.verbose) {
        console.log("Metric:    "+metric);
        console.log("  Agg:     "+aggregator);
        console.log("  Rate:    "+rate);
        console.log("  Down:    "+(downsampled ? downsampled : false));
        console.log("  Filters: "+JSON.stringify(filters));
    }

    if (!backend.performBackendQueries) {
        callback(null, {code:501,message:"Querying timeseries not supported by this backend"});
        return;
    }

    backend.performBackendQueries(startTime, endTime, downsampled, metric, filters, function(rawTimeSeries, err) {
        if (config.verbose) {
            console.log("received callback with params:");
            console.log("  " + JSON.stringify(rawTimeSeries));
            console.log("  " + JSON.stringify(err));
        }
        if (err) {
            if (config.verbose) {
                console.log("  Received error from backend: "+err);
            }
            callback(null, err);
            return;
        }
        if (config.verbose) {
            console.log("  Received "+rawTimeSeries.length+" raw time series from backend");
        }
        postBackendFiltering(rawTimeSeries, filters);
        if (config.verbose) {
            console.log("  Have "+rawTimeSeries.length+" raw time series after post- filtering");
        }

        var tagsets = constructUniqueTagSetsFromRawResults(rawTimeSeries, filters);
        if (config.verbose) {
            console.log("  Tsets:"+JSON.stringify(tagsets));
        }

        var ret = [];
        function processTagSet(s) {
            if (s<tagsets.length) {
                var participatingTimeSeries = rawTimeSeriesForTagSet(rawTimeSeries, tagsets[s]);
                if (config.verbose) {
                    console.log("  Have "+participatingTimeSeries.length+" time series for tagset "+s+" ("+JSON.stringify(tagsets[s])+")");
                }

                var aggregateTags = [];
                for (var p=0; p<participatingTimeSeries.length; p++) {
                    for (var k in participatingTimeSeries[p].tags) {
                        if (participatingTimeSeries[p].tags.hasOwnProperty(k)) {
                            var foundInTagSet = tagsets[s].hasOwnProperty(k);
                            if (!foundInTagSet) {
                                if (aggregateTags.indexOf(k) < 0) {
                                    aggregateTags.push(k);
                                }
                            }
                        }
                    }
                }
                aggregateTags.sort();
                if (config.verbose) {
                    console.log("  Aggregate tags: "+JSON.stringify(aggregateTags));
                }

                var downsampleNumberComponent = downsampled ? downsampled.match(/^[0-9]+/) : undefined;
                var downsampleStringComponent = downsampled ? downsampled.split("-")[0].match(/[a-zA-Z]+$/)[0] : undefined;
                var downsampleFunction = downsampled ? downsampled.split("-")[1] : "avg";
                var fillInPolicy = downsampled && downsampled.split("-").length > 2 ? downsampled.split("-")[2] : "none";
                var msMultiplier = 1000;
                switch (downsampleStringComponent) {
                    case 's': downsampleNumberComponent *= 1 * msMultiplier; break;
                    case 'm': downsampleNumberComponent *= 60 * msMultiplier; break;
                    case 'h': downsampleNumberComponent *= 3600 * msMultiplier; break;
                    case 'd': downsampleNumberComponent *= 86400 * msMultiplier; break;
                    case 'w': downsampleNumberComponent *= 7 * 86400 * msMultiplier; break;
                    case 'y': downsampleNumberComponent *= 365 * 86400 * msMultiplier; break;
                    default:
                        if (config.verbose && downsampled) {
                            console.log("unrecognized downsample unit: "+downsampleStringComponent);
                        }
                }
                if (!downsampled && !ms) {
                    downsampleNumberComponent = msMultiplier;
                }

                // todo: modify faketsdb - backend now works entirely in ms..
                var startTimeNormalisedToReturnUnits = startTime.getTime();
                var endTimeNormalisedToReturnUnits = endTime.getTime();
                var firstTimeStamp = startTimeNormalisedToReturnUnits % downsampleNumberComponent == 0 ? startTimeNormalisedToReturnUnits :
                    Math.floor((startTimeNormalisedToReturnUnits + downsampleNumberComponent) / downsampleNumberComponent) * downsampleNumberComponent;

                if (config.verbose) {
                    console.log("normalised startTime      = "+Math.floor(startTimeNormalisedToReturnUnits));
                    console.log("downsampleNumberComponent = "+downsampleNumberComponent);
                }

                if (participatingTimeSeries.length > 0) {
                    for (var p=participatingTimeSeries.length-1; p>=0; p--) {
                        if (participatingTimeSeries[p].dps.length === 0) {
                            participatingTimeSeries.splice(p, 1);
                        }
                    }

                    // downsample every timeseries before combining
                    if (downsampleNumberComponent !== undefined) {
                        for (var p = 0; p < participatingTimeSeries.length; p++) {
                            participatingTimeSeries[p].dps = downsampleSingleTimeSeriesPoints(participatingTimeSeries[p].dps, startTimeNormalisedToReturnUnits, endTimeNormalisedToReturnUnits, downsampleNumberComponent, downsampleFunction, fillInPolicy);
                        }
                    }

                    // combineTimeSeries performs aggregation, interpolation as required
                    var toPush = combineTimeSeries(participatingTimeSeries, tagsets[s], aggregateTags, firstTimeStamp, endTimeNormalisedToReturnUnits, metric, aggregator, ms, rate, arrays, showTsuids);

                    if (showQuery) {
                        toPush.query = query;
                    }

                    if (globalAnnotations) {
                        toPush.globalAnnotations = globalAnnotationsArray;
                    }

                    if (config.verbose) {
                        console.log("  Adding time series: "+JSON.stringify(toPush));
                    }
                    ret.push(toPush);

                    if (annotations) {
                        if (!backend.performAnnotationsQueries) {
                            callback(null, {code:501,message:"Querying annotations not supported by this backend"});
                            return;
                        }
                        backend.performAnnotationsQueries(startTime, endTime, downsampleNumberComponent, participatingTimeSeries, function(annotationsArray, err) {
                            annotationsArray.sort(function (a,b) {
                                var diff = a.startTime - b.startTime;
                                if (diff !== 0) {
                                    return diff;
                                }
                                var aEndTime = a == null ? 0 : a.endTime;
                                var bEndTime = b == null ? 0 : b.endTime;
                                return aEndTime - bEndTime;
                            });
                            toPush.annotations = annotationsArray;
                            processTagSet(s + 1)
                        });
                    }
                    else {
                        processTagSet(s + 1)
                    }
                }
                else {
                    processTagSet(s + 1);
                }
            }
            else {
                callback(ret);
            }
        }
        processTagSet(0);
    });
};

var queryImpl = function(start, end, mArray, arrays, ms, showQuery, annotations, globalAnnotations, showTsuids, res) {
    if (!start) {
        res.json("Missing start parameter");
        return;
    }
    if (config.verbose) {
        console.log("---------------------------");
    }

    var startTime = toDateTime(start);
    var endTime = end ? toDateTime(end) : new Date();
    if (config.verbose) {
        console.log("start     = "+start);
        console.log("end       = "+(end?end:""));
        console.log("startTime = "+startTime);
        console.log("endTime   = "+endTime);
    }

    var ret = [];

    function doNextSingleQuery(a, globalAnnotationsArray) {
        // m=<aggregator>:[rate[{counter[,<counter_max>[,<reset_value>]]]}:][<down_sampler>:]<metric_name>[{<tag_name1>=<grouping filter>[,...<tag_nameN>=<grouping_filter>]}][{<tag_name1>=<non grouping filter>[,...<tag_nameN>=<non_grouping_filter>]}]
        if (a<mArray.length) {
            performSingleMetricQuery(startTime, endTime, mArray[a], arrays, ms, showQuery, annotations, globalAnnotations, globalAnnotationsArray, showTsuids, function(series, err) {
                if (!handleErr(res, err)) {
                    ret = ret.concat(series);
                    //console.log("Added "+series.length+" series")
                    doNextSingleQuery(a + 1);
                }
            });
        }
        else {
            res.json(ret);
        }
    }

    if (globalAnnotations) {
        if (!backend.performGlobalAnnotationsQuery) {
            callback(null, {code:501,message:"Querying global annotations not supported by this backend"});
            return;
        }

        backend.performGlobalAnnotationsQuery(startTime, endTime, function(globalAnnotationsArray, err) {
            if (!handleErr(res, err)) {
                doNextSingleQuery(0, globalAnnotationsArray);
            }
        });
    }
    else {
        doNextSingleQuery(0, []);
    }
};

var unioningFunction = function(jsons, valueProvider) {

    // need to union tags across jsons`
    // fill value 0
    // each entry in jsons is an array of timeseries
    var seriesByTagSet = {};
    function key(ts) {
        return JSON.stringify(ts.tags); // todo: not stable
    }
    for (var j=0; j<jsons.length; j++) {
        for (var t=0; t<jsons[j].length; t++) {
            var k = key(jsons[j][t]);
            if (seriesByTagSet[k] == null) {
                seriesByTagSet[k] = [];
            }
            seriesByTagSet[k].push(jsons[j][t]);
        }
    }
    // union done, now to do our operations
    var ret = [];
    for (var k in seriesByTagSet) {
        if (seriesByTagSet.hasOwnProperty(k)) {
            var input = seriesByTagSet[k];
            var allTimes = {};
            for (var t=0; t<input.length; t++) {
                for (var time in input[t].dps) {
                    if (input[t].dps.hasOwnProperty(time) && !allTimes.hasOwnProperty(t)) {
                        allTimes[t] = t;
                    }
                }
            }
            var series = input[0];
            for (var time in allTimes) {
                if (allTimes.hasOwnProperty(time)) {
                    var value = valueProvider(time, input);
                    series.dps[time] = value;
                }
            }
            ret.push(series);
        }
    }
    return ret;
}

var gexpFunctions = {
    absolute: {
        maxMetrics: 1,
        extraArg: false,
        array_output: true,
        process: function(ms, jsons, _) {
            jsons = jsons[0];
            for (var j=0; j<jsons.length; j++) {
                for (var p=0; p<jsons[j].dps.length; p++) {
                    jsons[j].dps[p][1] = Math.abs(jsons[j].dps[p][1]);
                }
            }
            return jsons;
        }
    },
    scale: {
        maxMetrics: 1,
        extraArg: true,
        array_output: true,
        process: function(ms, jsons, extraArg) {
            jsons = jsons[0];
            for (var j=0; j<jsons.length; j++) {
                for (var p=0; p<jsons[j].dps.length; p++) {
                    jsons[j].dps[p][1] *= extraArg;
                }
            }
            return jsons;
        }
    },
    movingAverage: {
        maxMetrics: 1,
        extraArg: true,
        array_output: true,
        process: function(ms, jsons, extraArg) {
            jsons = jsons[0];
            var timeWindow = extraArg.substring(0, extraArg.length-1);
            var msMult = ms ? 1000 : 1;
            switch (extraArg[extraArg.length-1]) {
                case 's': 
                    timeWindow *= msMult;
                    break;
                case 'm': 
                    timeWindow *= msMult * 60;
                    break;
                case 'h': 
                    timeWindow *= msMult * 3600;
                    break;
                case 'd': 
                    timeWindow *= msMult * 86400;
                    break;
                case 'w': 
                    timeWindow *= msMult * 86400 * 7;
                    break;
                case 'n': 
                    timeWindow *= msMult * 86400 * 30;
                    break;
                case 'y': 
                    timeWindow *= msMult * 86400 * 365;
                    break;
            }
            for (var j=0; j<jsons.length; j++) {
                var sum = 0;
                var count = 0;
                var initialTime = 0;
                var initialIndex = 0;
                if (jsons[j].dps.length > 0) {
                    initialTime = jsons[j].dps[0][0];
                    sum = jsons[j].dps[0][1];
                    count = 1;
                }
                var p=1;
                for ( ; p<jsons[j].dps.length && jsons[j].dps[p][0] < initialTime + timeWindow; p++) {
                    sum += jsons[j].dps[p][1];
                    count ++;
                    jsons[j].dps[p][1] = sum / count;
                }
                // now we need to count out and in
                for ( ; p<jsons[j].dps.length; p++) {
                    sum += jsons[j].dps[p][1];
                    count ++;
                    var s = initialIndex;
                    for (; s<p; s++) {
                        if (jsons[j].dps[p][0] - jsons[j].dps[s][0] > timeWindow) {
                            sum -= jsons[j].dps[s][0];
                            count --;
                        }
                    }
                    jsons[j].dps[p][1] = sum / count;
                    initialIndex = s;
                }
            }
            return jsons;
        }
    },
    highestMax: {
        maxMetrics: 1,
        extraArg: true,
        array_output: true,
        process: function(ms, jsons, extraArg) {
            jsons = jsons[0];
            var maxes = [];
            for (var j=0; j<jsons.length; j++) {
                var max = null;
                if (jsons[j].dps.length > 0) {
                    max = jsons[j].dps[0][1];
                }
                for (var p=1; p<jsons[j].dps.length; p++) {
                    max = Math.max(jsons[j].dps[p][1], max);
                }
                maxes.push({index:j,max:max})
            }
            maxes.sort(function(a,b) {
                return b.max- a.max;
            });
            var lim = extraArg >= maxes.length ? maxes[maxes.length - 1] : maxes[extraArg-1];
            maxes.sort(function(a,b) {
                return a.index - b.index;
            });
            for (var j=jsons.length-1; j>=0; j--) {
                if (maxes[j].max < lim) {
                    jsons.splice(j, 1, []);
                }
            }
            for (var j=jsons.length-1; j>=0 && jsons.length > extraArg; j--) {
                if (maxes[j].max == lim) {
                    jsons.splice(j, 1, []);
                }
            }
            return jsons;
        }
    },
    highestCurrent: {
        maxMetrics: 1,
        extraArg: true,
        array_output: true,
        process: function(ms, jsons, extraArg) {
            jsons = jsons[0];
            var currents = [];
            for (var j=0; j<jsons.length; j++) {
                var current = jsons[j].dps.length > 0 ? jsons[j].dps[jsons[j].dps.length-1][1] : null;
                currents.push({index:j,current:currents})
            }
            currents.sort(function(a,b) {
                return b.current- a.current;
            });
            var lim = extraArg >= currents.length ? currents[currents.length - 1] : currents[extraArg-1];
            currents.sort(function(a,b) {
                return a.index - b.index;
            });
            for (var j=jsons.length-1; j>=0; j--) {
                if (currents[j].current < lim) {
                    jsons.splice(j, 1, []);
                }
            }
            for (var j=jsons.length-1; j>=0 && jsons.length > extraArg; j--) {
                if (currents[j].current == lim) {
                    jsons.splice(j, 1, []);
                }
            }
            return jsons;
        }
    },
    diffSeries: {
        maxMetrics: 26,
        extraArg: false,
        array_output: false,
        process: function(ms, jsons, extraArg) {
            // subtract series 2 through n from 1 (according to graphite docs)
            return unioningFunction(jsons, function(time, timeseries) {
                var value = 0;
                if (timeseries[0].dps.hasOwnProperty(time)) {
                    value = timeseries[0].dps[time];
                }
                var sumOthers = 0;
                for (var t=1; t<timeseries.length; t++) {
                    if (timeseries[t].dps.hasOwnProperty(time)) {
                        sumOthers += timeseries[t].dps[time];
                    }
                }
                return value - sumOthers;
            });
        }
    },
    sumSeries: {
        maxMetrics: 26,
        extraArg: false,
        array_output: false,
        process: function(ms, jsons, extraArg) {
            return unioningFunction(jsons, function(time, timeseries) {
                var value = 0;
                for (var t=0; t<timeseries.length; t++) {
                    if (timeseries[t].dps.hasOwnProperty(time)) {
                        value += timeseries[t].dps[time];
                    }
                }
                return value;
            });
        }
    },
    multiplySeries: {
        maxMetrics: 26,
        extraArg: false,
        array_output: false,
        process: function(ms, jsons, extraArg) {
            return unioningFunction(jsons, function(time, timeseries) {
                var value = 1;
                for (var t=0; t<timeseries.length; t++) {
                    if (timeseries[t].dps.hasOwnProperty(time)) {
                        value *= timeseries[t].dps[time];
                    }
                    else {
                        value *= 0;
                    }
                }
                return value;
            });
        }
    },
    divideSeries: {
        maxMetrics: 26,
        extraArg: false,
        array_output: false,
        process: function(ms, jsons, extraArg) {
            // dividendSeriesList, divisor (according to graphite)
            // need to check tsdb code
            return null;
        }
    }/*, TODO - undocumented function - note there's a bug in 2.3 - do we want configurable bug compatibility?
    timeShift: {
        maxMetrics: 26,
        extraArg: false,
        array_output: false,
        process: function(ms, jsons, extraArg) {
            // dividendSeriesList, divisor (according to graphite)
            // need to check tsdb code
            return null;
        }
    }*/
}

var gexpQueryImpl = function(start, end, eArray, arrays, ms, showQuery, annotations, globalAnnotations, showTsuids, res) {
    if (!start) {
        res.json("Missing start parameter");
        return;
    }
    if (config.verbose) {
        console.log("---------------------------");
    }

    var startTime = toDateTime(start);
    var endTime = end ? toDateTime(end) : new Date();
    if (config.verbose) {
        console.log("start     = "+start);
        console.log("end       = "+(end?end:""));
        console.log("startTime = "+startTime);
        console.log("endTime   = "+endTime);
    }

    var seed = start + (end ? end : "");
    var rand = new Math.seedrandom(seed);

    var ret = [];
    
    var globalAnnotationsArray = [];
    if (globalAnnotations) {
        // populate some global annotations
        var from = startTime.getTime();
        var to = endTime.getTime();
        for (var t=from; t<to; ) {
            if (rand() <= config.probabilities.globalAnnotation) {
                var ann = {
                    "description": "Notice",
                    "notes": "DAL was down during this period",
                    "custom": null,
                    "endTime": t+((to-from)/20),
                    "startTime": t
                };
                globalAnnotationsArray.push(ann);
            }
            
            // next time
            var inc = rand() * ((to-from)/3);
            inc += "";
            if (inc.indexOf(".") > -1) {
                inc = inc.substring(0, inc.indexOf("."));
            }
            
            t += parseInt(inc);
        }
    }

    // m=<aggregator>:[rate[{counter[,<counter_max>[,<reset_value>]]]}:][<down_sampler>:]<metric_name>[{<tag_name1>=<grouping filter>[,...<tag_nameN>=<grouping_filter>]}][{<tag_name1>=<non grouping filter>[,...<tag_nameN>=<non_grouping_filter>]}]
    for (var a=0; a<eArray.length; a++) {
        /*
        Currently supported functions: 

         absolute(<metric>)

         Emits the results as absolute values, converting negative values to positive.
         diffSeries(<metric>[,<metricN>])

         Returns the difference of all series in the list. Performs a UNION across tags in each metric result sets, defaulting to a fill value of zero. A maximum of 26 series are supported at this time.
         divideSeries(<metric>[,<metricN>])

         Returns the quotient of all series in the list. Performs a UNION across tags in each metric result sets, defaulting to a fill value of zero. A maximum of 26 series are supported at this time.
         highestCurrent(<metric>,<n>)

         Sorts all resulting time series by their most recent value and emits n number of series with the highest values. n must be a positive integer value.
         highestMax(<metric>,<n>)

         Sorts all resulting time series by the maximum value for the time span and emits n number of series with the highest values. n must be a positive integer value.
         movingAverage(<metric>,<window>)

         Emits a sliding window moving average for each data point and series in the metric. The window parameter may either be a positive integer that reflects the number of data points to maintain in the window (non-timed) or a time span specified by an integer followed by time unit such as `60s` or `60m` or `24h`. Timed windows must be in single quotes.
         multiplySeries(<metric>[,<metricN>])

         Returns the product of all series in the list. Performs a UNION across tags in each metric result sets, defaulting to a fill value of zero. A maximum of 26 series are supported at this time.
         scale(<metric>,<factor>)

         Multiplies each series by the factor where the factor can be a positive or negative floating point or integer value.
         sumSeries(<metric>[,<metricN>])

         Returns the sum of all series in the list. Performs a UNION across tags in each metric result sets, defaulting to a fill value of zero. A maximum of 26 series are supported at this time.

         */
        var exp = eArray[a];
        
        console.log("Parsing expression: "+exp);
        
        var firstBracket = exp.indexOf("(");
        if (firstBracket == -1 || !gexpFunctions.hasOwnProperty(exp.substring(0, firstBracket))) {
            // todo: error
        }
        
        var func = exp.substring(0, firstBracket);
        var index = func.length + 1;
        
        function calcNextCommaOrEnd(exp, fromIndex) {
            return Math.min(exp.indexOf(",", fromIndex) == -1 ? 10000000 : exp.indexOf(",", fromIndex), exp.indexOf(")", fromIndex));
        }
        
        
        var metrics = [];
        /*
sumSeries(sum:cpu.percent{host=*},sum:ifstat.bytes{host=*})
         index = 59
         nextCommaOrEnd = -1
         nextBrace = -1
         closeBrace = 32
         nextCommaOrEnd = 33
         nextBrace = 50
*/
        for (var i=0; i<gexpFunctions[func].maxMetrics && index<exp.length && exp[index] != ")"; i++) {
//            console.log("index = "+index);
            // parse metric - janky, but should work
            var nextCommaOrEnd = calcNextCommaOrEnd(exp, index);
            var nextBrace = exp.indexOf("{", index);
//            console.log("nextCommaOrEnd = "+nextCommaOrEnd);
//            console.log("nextBrace = "+nextBrace);
            if (nextCommaOrEnd < nextBrace) {
                var candidateMetric = exp.substring(index, nextCommaOrEnd);
                if (candidateMetric.indexOf("rate") == -1) {
                    metrics.push(exp.substring(index, nextCommaOrEnd).trim());
                    index = nextCommaOrEnd + 1;
                }
                else {
                    var nextColon = exp.indexOf(":", nextCommaOrEnd);
                    nextCommaOrEnd = calcNextCommaOrEnd(exp, nextColon);
                    if (nextCommaOrEnd < nextBrace) {
                        metrics.append(exp.substring(index, nextCommaOrEnd)).trim();
                        index = nextCommaOrEnd + 1;
                    }
                }
            }
            else {
                var closeBrace = exp.indexOf("}", nextBrace+1);
//                console.log("closeBrace = "+closeBrace);
                nextCommaOrEnd = calcNextCommaOrEnd(exp, closeBrace+1);
//                console.log("nextCommaOrEnd = "+nextCommaOrEnd);
                nextBrace = exp.indexOf("{", closeBrace+1);
//                console.log("nextBrace = "+nextBrace);
                if (nextCommaOrEnd < nextBrace || nextBrace === -1) {
                    metrics.push(exp.substring(index, nextCommaOrEnd).trim());
                    index = nextCommaOrEnd + 1;
                }
                else {
                    closeBrace = exp.indexOf("}", nextBrace + 1);
//                    console.log("closeBrace = "+closeBrace);
                    nextCommaOrEnd = calcNextCommaOrEnd(exp, closeBrace+1);
//                    console.log("nextCommaOrEnd = "+nextCommaOrEnd);
                    metrics.push(exp.substring(index, nextCommaOrEnd).trim());
                    index = nextCommaOrEnd + 1;
                }
            }
        }
        
        // done reading metrics
        console.log("func = "+func);
        
        // now we might have other args
        var extraArg = null;
        if (gexpFunctions[func].extraArg) {
            extraArg = exp.substring(index, exp.indexOf(")", index)).trim();
            console.log("extraArg = "+extraArg);
            if (extraArg === "") {
                // todo: error
            }
        }
        
        // now process
        var jsons = [];
        for (var m=0; m<metrics.length; m++) {
            console.log("Processing metric: "+metrics[m]);
            jsons.push(performSingleMetricQuery(startTime, endTime, rand, metrics[m], gexpFunctions[func].array_output, ms, showQuery, annotations, globalAnnotations, globalAnnotationsArray, showTsuids));
        }
        var mappedResults = gexpFunctions[func].process(ms, jsons, extraArg);
        for (var m=0; m<mappedResults.length; m++) {
            mappedResults[m].metric = exp;
        }
        if (arrays && !gexpFunctions[func].array_output) {
            // convert mapped to arrays
            for (var m=0; m<mappedResults.length; m++) {
                var dps = [];
                for (var k in mappedResults[m].dps) {
                    if (mappedResults[m].dps.hasOwnProperty(k)) {
                        dps.push([k, mappedResults[m].dps[k]]);
                    }
                }
                dps.sort(function (a,b) {
                    return a[0] - b[0];
                });
                mappedResults[m].dps = dps;
            }
        }
        else if (!arrays && gexpFunctions[func].array_output) {
            // convert arrays to mapped
            // convert mapped to arrays
            for (var m=0; m<mappedResults.length; m++) {
                var dps = {};
                for (var p=0; p<mappedResults[m].dps.length; p++) {
                    dps[mappedResults[m].dps[p][0]] = mappedResults[m].dps[p][1]; 
                }
                
                mappedResults[m].dps = dps;
            }
        }
        ret = ret.concat(mappedResults);
    }


    res.json(ret);
};

var queryGet = function(req, res) {
    var queryParams = req.query;
    var arrayResponse = queryParams["arrays"] && queryParams["arrays"]==="true";
    var showQuery = queryParams["show_query"] && queryParams["show_query"]==="true";
    var showTsuids = queryParams["show_tsuids"] && queryParams["show_tsuids"]==="true";
    var showAnnotations = !(queryParams["no_annotations"] && queryParams["no_annotations"]==="true");
    var globalAnnotations = queryParams["global_annotations"] && queryParams["global_annotations"]==="true";
    var mArray = queryParams["m"];
    mArray = [].concat( mArray );
    queryImpl(queryParams["start"],queryParams["end"],mArray,arrayResponse,queryParams["ms"],showQuery,showAnnotations,globalAnnotations,showTsuids,res);
};

var queryPost = function(req, res) {
    var queryParams = req.body;
    var arrayResponse = queryParams["arrays"] && queryParams["arrays"]===true;
    var showQuery = queryParams["show_query"] && queryParams["show_query"]===true;
    var showTsuids = queryParams["show_tsuids"] && queryParams["show_tsuids"]===true;
    var showAnnotations = !(queryParams["no_annotations"] && queryParams["no_annotations"]===true);
    var globalAnnotations = queryParams["global_annotations"] && queryParams["global_annotations"]===true;
    var mArray = [];
    var queries = [].concat(queryParams["queries"]);
    for (var q=0; q<queries.length; q++) {
        var query = queries[q];
        var m = query.aggregator;
        if (query["rate"] && query["rate"]==="true") {
            var rateString = "rate";
            if (query["rateOptions"]) {
                var rateOptions = query["rateOptions"];
                if (rateOptions["counter"] && rateOptions["counter"] === "true") {
                    rateString += "{counter,";
                    if (rateOptions["counterMax"]) {
                        rateString += rateOptions["counterMax"];
                    }
                    rateString += ",";
                    if (rateOptions["resetValue"]) {
                        rateString += rateOptions["resetValue"];
                    }
                    if (rateOptions["dropResets"] && rateOptions["dropResets"] === "true") {
                        rateString += ",true";
                    }
                    rateString += "}";
                }
            }
            m += ":" + rateString;
        }
        if (query["downsample"]) {
            m += ":" + query.downsample;
        }
        m += ":" + query.metric;
        m += "{";
        var tags = query["tags"];
        var filters = query["filters"];
        var sep = "";
        if (tags) {
            for (var key in tags) {
                if (tags.hasOwnProperty(key)) {
                    m += sep + key + "=" + tags[key];
                    sep = ",";
                }
            }
        }
        if (filters) {
            for (var f=0; f<filters.length; f++) {
                var filter = filters[f];
                if (filter["group_by"]) {
                    m += sep + filter.tagk + "=" + filter.type + "(" + filter.filter + ")";
                    sep = ",";
                }
            }
        }
        m += "}";
        if (filters) {
            sep = "{";
            for (var f=0; f<filters.length; f++) {
                var filter = filters[f];
                if (!filter["group_by"]) {
                    m += sep + filter.tagk + "=" + filter.type + "(" + filter.filter + ")";
                    sep = ",";
                }
            }
            if (sep !== "}") {
                m += "}";
            }
        }
        mArray.push(m);
    }
    queryImpl(queryParams["start"],queryParams["end"],mArray,arrayResponse,queryParams["ms"],showQuery,showAnnotations,globalAnnotations,showTsuids,res);
};

var gexpQueryGet = function(req, res) {
    var queryParams = req.query;
    var arrayResponse = queryParams["arrays"] && queryParams["arrays"]==="true";
    var showQuery = queryParams["show_query"] && queryParams["show_query"]==="true";
    var showTsuids = queryParams["show_tsuids"] && queryParams["show_tsuids"]==="true";
    var showAnnotations = !(queryParams["no_annotations"] && queryParams["no_annotations"]==="true");
    var globalAnnotations = queryParams["global_annotations"] && queryParams["global_annotations"]==="true";
    var eArray = queryParams["exp"];
    eArray = [].concat( eArray );
    gexpQueryImpl(queryParams["start"],queryParams["end"],eArray,arrayResponse,queryParams["ms"],showQuery,showAnnotations,globalAnnotations,showTsuids,res);
};

var versionGet = function(req, res) {
    res.json({
        "timestamp": "1362712695",
        "host": "localhost",
        "repo": "/opt/opentsdb/build",
        "full_revision": "11c5eefd79f0c800b703ebd29c10e7f924c01572",
        "short_revision": "11c5eef",
        "user": "localuser",
        "repo_status": "MODIFIED",
        "version": config.version
    });
};

var configGet = function(req, res) {
    res.json({});
};



var suggestImpl = function(req, res) {
    var queryParams = req.query;
    var max = queryParams["max"];
    var q = (!queryParams["q"] || queryParams["q"] === "") ? null : queryParams["q"];
    switch (queryParams["type"]) {
        case "metrics":
            if (!backend.suggestMetrics) {
                res.json(null, {code:501,message:"Suggesting metrics not supported by this backend"});
                return;
            }
            backend.suggestMetrics(q, max, function(result, err) {
                if (!handleErr(res, err)) {
                    res.json(result);
                }
            });
            break;
        case "tagk":
            if (!backend.suggestTagKeys) {
                res.json(null, {code:501,message:"Suggesting tag keys not supported by this backend"});
                return;
            }
            backend.suggestTagKeys(q, max, function(result, err) {
                if (!handleErr(res, err)) {
                    res.json(result);
                }
            });
            break;
        case "tagv":
            if (!backend.suggestTagValues) {
                res.json(null, {code:501,message:"Suggesting tag values not supported by this backend"});
                return;
            }
            backend.suggestTagValues(q, max, function(result, err) {
                if (!handleErr(res, err)) {
                    res.json(result);
                }
            });
            break;
        default:
            res.json({message:"Unrecognised query type: "+queryParams["type"]});

    }
};

var apiResources = {
    "/aggregators": {
        "GET": aggregatorsImpl,
        "POST": aggregatorsImpl
    },
    "/annotation": {
        "POST": annotationPostImpl,
        "DELETE": annotationDeleteImpl
    },
    "/annotation/bulk": {
        "POST": annotationBulkPostImpl
    },
    "/config": {
        "GET": configGet
    },
    "/put": {
        "POST": putImpl
    },
    "/query": {
        "GET": queryGet,
        "POST": queryPost
    },
    "/query/gexp": {
        "GET": gexpQueryGet
    },
    "/search/lookup": {
        "GET": searchLookupGet,
        "POST": searchLookupPost
    },
    "/suggest": {
        "GET": suggestImpl
    },
    "/uid/uidmeta": {
        "GET": uidMetaGet
    },
    "/version": {
        "GET": versionGet,
        "POST": versionGet
    }
};

var apiGateway = function(req, res) {
    if (apiResources.hasOwnProperty(req.path)) {
        var methods = apiResources[req.path];
        if (methods.hasOwnProperty(req.method)) {
            methods[req.method](req, res);
        }
        else {
            res.send(405);
        }
    }
    else {
        res.send(404);
    }
};

// all routes exist here so we know what's implemented
for (var p in apiResources) {
    if (apiResources.hasOwnProperty(p)) {
        var methods = apiResources[p];
        if (methods.hasOwnProperty("GET")) {
            router.get(p, methods["GET"]);
        }
        if (methods.hasOwnProperty("POST")) {
            router.post(p, bodyParser.json(), methods["POST"]);
        }
        if (methods.hasOwnProperty("DELETE")) {
            router.delete(p, methods["DELETE"]);
        }
    }
}

var applyOverrides = function(from, to) {
    for (var k in from) {
        if (from.hasOwnProperty(k)) {
            if (to.hasOwnProperty(k)) {
                switch (typeof from[k]) {
                    case 'number':
                    case 'string':
                    case 'boolean':
                        to[k] = from[k];
                        continue;
                    default:
                        console.log("unhandled: "+(typeof from[k]));
                }
                applyOverrides(from[k], to[k]);
            }
            else {
                to[k] = from[k];
            }
        }
    }
};

var setupTsdbApi = function(incomingConfig) {
    if (!incomingConfig) {
        incomingConfig = {};
    }

    var conf = {
        verbose: false,
        logRequests: true,
        version: "2.2.0"
    };

    applyOverrides(incomingConfig, conf);

    config = conf;
};

var installTsdbApi = function(app, incomingConfig) {
    setupTsdbApi(incomingConfig);

    if (config.logRequests) {

        // middleware specific to this router
        router.use(function timeLog(req, res, next) {
            console.log(new Date(Date.now())+': '+req.originalUrl);
            next();
        });
    }
    app.use('/api',router);
};

var setBackend = function(b) {
    backend = b;
};

var createApiGateway = function(backend, conf) {
    setupTsdbApi(conf);
    setBackend(backend);
    var corsFn = cors();
    return function(req, res) {
        corsFn(req, res, function() {
            apiGateway(req, res);
        });
    }
};

module.exports = {
    backend: setBackend,
    install: installTsdbApi,
    apiGateway: createApiGateway
};