var express = require('express');
var bodyParser = require('body-parser');
var router = express.Router();
router.use(bodyParser.json());
require('seedrandom');
var moment = require('moment');

var backend = undefined;
// public interface:
//   uidMetaFromName(type, name)
//   backend.uidMetaFromUid(type, uid);
//   backend.searchLookupImpl(metric, limit, useMeta);
//   backend.performBackendQueries(startTime, endTime, ms, downsampled, metric, filters);
//   backend.performAnnotationsQueries(startTime, endTime, participatingTimeSeries);
//   backend.suggestMetrics(query)

var uid = function(type, name) {
    var meta = backend.uidMetaFromName(type, name);
    if (meta != null) {
        return meta.uid;
    }
    return null;
}

var aggregatorsImpl = function(req, res) {
    // if add more here then add support in query implementation
    res.json(["avg","sum","min","max"]);
}

var tsuid = function(metric, tags) {
    var ret = uid("metric", metric);
    for (var k in tags) {
        if (tags.hasOwnProperty(k)) {
            ret += uid("tagk", k) + uid("tagv", tags[k]);
        }
    }
    return ret;
}

var annotationPostImpl = function(req, res) {
    res.json(req.body);
}

var annotationDeleteImpl = function(req, res) {
    res.json(req.body);
}

var annotationBulkPostImpl = function(req, res) {
    res.json(req.body);
}
var searchLookupPost = function(req, res) {
    var metric = req.body.metric;
    var limit = req.body.limit;
    var results = backend.searchLookupImpl(metric, limit, req.body.useMeta);
    var ret = {
        "type": "LOOKUP",
        "metric": metric,
        "limit": limit,
        "time": 1,
        "results": results,
        "startIndex": 0,
        "totalResults": 0
    };
    res.json(ret);
}
var searchLookupGet = function(req, res) {
    try {
        var queryParams = req.query;
        var metric = queryParams["m"];
        var limit = queryParams["limit"];
        var results = backend.searchLookupImpl(metric, limit, queryParams["use_meta"]);
        var ret = {
            "type": "LOOKUP",
            "metric": metric,
            "limit": limit,
            "time": 1,
            "results": results,
            "startIndex": 0,
            "totalResults": results.length
        };
        res.json(ret);
    }
    catch (e) {
        console.log(e);
    }
}

var uidMetaGet = function(req, res) {
    var queryParams = req.query;

    var meta = backend.uidMetaFromUid(queryParams["type"], queryParams["uid"]);
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
            message: queryParams["type"] + " with uid "+queryParams["uid"]+" not found"
        });
    }

}

var toDateTime = function(tsdbTime) {
    if (tsdbTime.indexOf("ago")<0) {
        if (tsdbTime.indexOf("/") >= 0) {
            return moment(tsdbTime, "YYYY/MM/DD HH:mm:ss").toDate();
        }
        return new Date(tsdbTime > 10000000000 ? tsdbTime : tsdbTime * 1000);
    }

    if (tsdbTime == null || tsdbTime == "") {
        return new Date();
    }

    tsdbTime = tsdbTime.split("-")[0];
    var numberComponent = tsdbTime.match(/^[0-9]+/);
    var stringComponent = tsdbTime.match(/[a-zA-Z]+$/);
    if (numberComponent.length == 1 && stringComponent.length == 1) {
        return moment().subtract(numberComponent[0], stringComponent[0]).toDate();
    }
    return new Date();
}

var sum = function(arr) {
    var ret = 0;
    for (var i=0; i<arr.length; i++) {
        ret += arr[i];
    }
    return ret;
}

var min = function(arr) {
    var ret = null;
    for (var i=0; i<arr.length; i++) {
        if (!ret) {
            ret = arr[i];
        }
        else {
            ret = Math.min(arr[i], ret);
        }
    }
    return ret;
}

var max = function(arr) {
    var ret = null;
    for (var i=0; i<arr.length; i++) {
        if (!ret) {
            ret = arr[i];
        }
        else {
            ret = Math.max(arr[i], ret);
        }
    }
    return ret;
}



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
        if (filter.type == "literal_or" || filter.type == "iliteral_or"
            || filter.type == "not_literal_or" || filter.type == "not_iliteral_or") {
            ignoreCase = filter.type.indexOf("iliteral") >= 0;
            negate = filter.type.indexOf("not_") == 0;
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
        if (filter.type == "wildcard" || filter.type == "iwildcard" || filter.type == "regexp") {
            ignoreCase = filter.type == "iwildcard";
            var matchAll = (filter.type.indexOf("wildcard") >= 0 && filter.filter == "*")
                        || (filter.type == "regexp" && filter.filter == ".*");
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
                        if (regexp != "*") {
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
                    continue;
                }
            }
        }
    }   
}

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
    
    if (ret.length == 0) {
        ret.push({});
    }
    return ret;
}

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
}

var performSingleMetricQuery = function(startTime, endTime, rand, m, arrays, ms, showQuery, annotations, globalAnnotations, globalAnnotationsArray, showTsuids) {
    var colonSplit = m.split(":");
    var aggregator = colonSplit[0];
    var rate = false;
    var downsampled = false;
    if (colonSplit[1].indexOf("rate") == 0) {
        rate = true;
        // todo: consider supporting counters?
        if (colonSplit.length == 4) {
            downsampled = colonSplit[2];
        }
    }
    else {
        if (colonSplit.length == 3) {
            downsampled = colonSplit[1];
        }
    }
    var metricAndTags = colonSplit[colonSplit.length-1];
    var metric = metricAndTags;
    var tags = [];
    var filters = [];
    var openCurly = metricAndTags.indexOf("{");
    var closeCurly = metricAndTags.indexOf("}");
    if (openCurly >= 0) {
        metric = metricAndTags.substring(0, openCurly);
        var tagString = metricAndTags.substring(openCurly+1);
        tagString = tagString.substring(0, tagString.length-1);

        // some idiot specified the tag spec as {}
        if (tagString != "") {
            var tagArray = tagString.split(",");
            for (var t=0; t<tagArray.length; t++) {
                var kv = tagArray[t].split("=");
                if (kv[1].indexOf("*")>=0) {
                    tags.push({tagk:kv[0],tagv:allTagValues(metric, kv[0])});
                    filters.push({tagk:tags[t].tagk,type:"wildcard",filter:[kv[0]],group_by:true});
                }
                else if (kv[1].indexOf("|")>=0) {
                    tags.push({tagk:kv[0],tagv:kv[1].split("|")});
                    filters.push({tagk:tags[t].tagk,type:"literal_or",filter:kv[1].split("|"),group_by:true});
                }
                else {
                    tags.push({tagk:kv[0],tagv:[kv[1]]});
                    filters.push({tagk:tags[t].tagk,type:"literal_or",filter:[kv[1]],group_by:true});
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
        console.log("Metric: "+metric);
        console.log("  Agg:  "+aggregator);
        console.log("  Rate: "+rate);
        console.log("  Down: "+(downsampled ? downsampled : false));
        console.log("  Tags: "+JSON.stringify(tags));
    }
    
    var rawTimeSeries = backend.performBackendQueries(startTime, endTime, ms, downsampled, metric, filters);
    postBackendFiltering(rawTimeSeries, filters);
    
    var tagsets = constructUniqueTagSetsFromRawResults(rawTimeSeries, filters);

//    tagsets = constructUniqueTagSets(tags);

    if (config.verbose) {
        console.log("  Tsets:"+JSON.stringify(tagsets));
    }

    var ret = [];
    for (var s=0; s<tagsets.length; s++) {

        var participatingTimeSeries = rawTimeSeriesForTagSet(rawTimeSeries, tagsets[s]);
        /*
        participatingTimeSeries = [];
        for (var t=0; t<timeseries.length; t++) {
            if (timeseries[t].metric == metric) {
                var participating = true;
                for (var i=0; i<tags.length; i++) {
                    if (timeseries[t].tags.hasOwnProperty(tags[i].tagk)) {
                        var ind = tags[i].tagv.indexOf(timeseries[t].tags[tags[i].tagk]);
                        if (ind < 0) {
                            participating = false;
                            break;
                        }
                    }
                }
                if (participating) {
                    if (config.verbose) {
                        console.log("    Participant: "+t);
                    }
                    participatingTimeSeries.push(timeseries[t]);
                }
            }
        }*/

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

        var downsampleNumberComponent = downsampled ? downsampled.match(/^[0-9]+/) : 10;
        var downsampleStringComponent = downsampled ? downsampled.split("-")[0].match(/[a-zA-Z]+$/)[0] : "s";
        var msMultiplier = ms ? 1000 : 1;
        switch (downsampleStringComponent) {
            case 's': downsampleNumberComponent *= 1 * msMultiplier; break;
            case 'm': downsampleNumberComponent *= 60 * msMultiplier; break;
            case 'h': downsampleNumberComponent *= 3600 * msMultiplier; break;
            case 'd': downsampleNumberComponent *= 86400 * msMultiplier; break;
            case 'w': downsampleNumberComponent *= 7 * 86400 * msMultiplier; break;
            case 'y': downsampleNumberComponent *= 365 * 86400 * msMultiplier; break;
            default:
                if (config.verbose) {
                    console.log("unrecognized downsample unit: "+downsampleStringComponent);
                }
        }

        var startTimeNormalisedToReturnUnits = ms ? startTime.getTime() : startTime.getTime() / 1000;
        var endTimeNormalisedToReturnUnits = ms ? endTime.getTime() : endTime.getTime() / 1000;
        var firstTimeStamp = startTimeNormalisedToReturnUnits % downsampleNumberComponent == 0 ? startTimeNormalisedToReturnUnits :
            Math.floor((startTimeNormalisedToReturnUnits + downsampleNumberComponent) / downsampleNumberComponent) * downsampleNumberComponent;

        if (config.verbose) {
            console.log("normalised startTime      = "+Math.floor(startTimeNormalisedToReturnUnits));
            console.log("downsampleNumberComponent = "+downsampleNumberComponent);
        }

        if (participatingTimeSeries.length > 0) {
            var annotationsArray = backend.performAnnotationsQueries(startTime, endTime, participatingTimeSeries);
            var tsuids = [];
            for (var p=0; p<participatingTimeSeries.length; p++) {
                tsuids.push(participatingTimeSeries[p].tsuid);
                if (config.verbose) {
                    console.log("data = "+JSON.stringify(participatingTimeSeries[p].dps));
                }
            }

            for (var p=participatingTimeSeries.length-1; p>=0; p--) {
                if (participatingTimeSeries[p].dps.length == 0) {
                    participatingTimeSeries.splice(p, 1);
                }
            }

            // now combine data as appropriate
            var combinedDps = arrays ? [] : {};
            var indices = new Array(participatingTimeSeries.length);
            for (var i=0; i<indices.length; i++) {
                indices[i] = 0;
            }
            if (config.verbose) {
                console.log("    combining "+indices.length+" participating time series");
            }
            for (var t=firstTimeStamp; t<=endTimeNormalisedToReturnUnits; t+=downsampleNumberComponent) {
                if (config.verbose) {
                    console.log("     t = "+t);
                }
                var points = [];
                for (var i=0; i<indices.length; i++) {
                    while (indices[i]<participatingTimeSeries[i].dps.length && participatingTimeSeries[i].dps[indices[i]][0]<t) {
                        indices[i]++;
                    }
                    if (indices[i]<participatingTimeSeries[i].dps.length && participatingTimeSeries[i].dps[indices[i]][0] == t) {
                        if (config.verbose) {
                            console.log("     indices["+i+"] = "+JSON.stringify(indices[i]));
                        }
                        if (indices[i]<participatingTimeSeries[i].dps.length) {
                            if (participatingTimeSeries[i].dps[indices[i]][0]==t) {
                                if (config.verbose) {
                                    console.log("     a");
                                }
                                points.push(participatingTimeSeries[i].dps[indices[i]][1]);
                            }
                            else { // next dp time is greater than time desired
                                if (config.verbose) {
                                    console.log("     b");
                                }
                                // can't interpolate from before beginning
                                if (indices[i]>0) {
                                    if (config.verbose) {
                                        console.log("     c");
                                    }
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
                }
                if (config.verbose) {
                    console.log("      For time "+t+", partipating points = "+JSON.stringify(points));
                }
                // now we have our data points, combine them:
                if (points.length > 0) {
                    switch (aggregator) {
                        case "sum":
                            val = sum(points);
                            break;
                        case "avg":
                            val = sum(points)/participatingTimeSeries.length;
                            break;
                        case "min":
                            val = sum(points);
                            break;
                        case "max":
                            val = sum(points);
                            break;
                        default:
                            throw "unrecognized agg: "+aggregator;
                    }
                    if (arrays) {
                        combinedDps.push([t,val]);
                    }
                    else {
                        combinedDps[t] = val;
                    }
                }
            }

            var toPush = {
                "metric": metric,
                "tags": tagsets[s],
                "aggregatedTags": aggregateTags,
                "dps": combinedDps
            };

            if (showQuery) {
                toPush.query = query;
            }

            if (annotations) {
                toPush.annotations = annotationsArray;
            }

            if (globalAnnotations) {
                toPush.globalAnnotations = globalAnnotationsArray;
            }

            if (showTsuids) {
                toPush.tsuids = tsuids;
            }

            ret.push(toPush);
            if (config.verbose) {
                console.log("Adding time series");
            }
        }
    }
    return ret;
} 

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
    for (var a=0; a<mArray.length; a++) {
        var series = performSingleMetricQuery(startTime, endTime, rand, mArray[a], arrays, ms, showQuery, annotations, globalAnnotations, globalAnnotationsArray, showTsuids);
        ret = ret.concat(series);
        //console.log("Added "+series.length+" series")
    }

    res.json(ret);
}

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
            var dps = {};
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
    }
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
                if (nextCommaOrEnd < nextBrace || nextBrace == -1) {
                    metrics.push(exp.substring(index, nextCommaOrEnd).trim());
                    index = nextCommaOrEnd + 1;
                }
                else {
                    closeBrace = exp.indexOf("}", nextBrace+1);
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
            if (extraArg == "") {
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
}

var queryGet = function(req, res) {
    var queryParams = req.query;
    var arrayResponse = queryParams["arrays"] && queryParams["arrays"]=="true";
    var showQuery = queryParams["show_query"] && queryParams["show_query"]=="true";
    var showTsuids = queryParams["show_tsuids"] && queryParams["show_tsuids"]=="true";
    var showAnnotations = !(queryParams["no_annotations"] && queryParams["no_annotations"]=="true");
    var globalAnnotations = queryParams["global_annotations"] && queryParams["global_annotations"]=="true";
    var mArray = queryParams["m"];
    mArray = [].concat( mArray );
    queryImpl(queryParams["start"],queryParams["end"],mArray,arrayResponse,queryParams["ms"],showQuery,showAnnotations,globalAnnotations,showTsuids,res);
}

var gexpQueryGet = function(req, res) {
    var queryParams = req.query;
    var arrayResponse = queryParams["arrays"] && queryParams["arrays"]=="true";
    var showQuery = queryParams["show_query"] && queryParams["show_query"]=="true";
    var showTsuids = queryParams["show_tsuids"] && queryParams["show_tsuids"]=="true";
    var showAnnotations = !(queryParams["no_annotations"] && queryParams["no_annotations"]=="true");
    var globalAnnotations = queryParams["global_annotations"] && queryParams["global_annotations"]=="true";
    var eArray = queryParams["exp"];
    eArray = [].concat( eArray );
    gexpQueryImpl(queryParams["start"],queryParams["end"],eArray,arrayResponse,queryParams["ms"],showQuery,showAnnotations,globalAnnotations,showTsuids,res);
}

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
}

var configGet = function(req, res) {
    res.json({});
}



var suggestImpl = function(req, res) {
    var queryParams = req.query;
    if (queryParams["type"] == "metrics") {
        if (!queryParams["q"] || queryParams["q"] == "") {
            res.json(backend.suggestMetrics());
        }
        else {
            res.json(backend.suggestMetrics(queryParams["q"]));
        }
        return;
    }
    throw 'unhandled response';
};

// all routes exist here so we know what's implemented
router.get('/suggest', suggestImpl);
router.get('/aggregators', aggregatorsImpl);
router.post('/aggregators', aggregatorsImpl);
router.get('/search/lookup', searchLookupGet);
router.post('/search/lookup', bodyParser.json(), searchLookupPost);
router.post('/annotation', annotationPostImpl);
router.delete('/annotation', annotationDeleteImpl);
router.post('/annotation/bulk', annotationBulkPostImpl);
router.get('/query', queryGet);
router.get('/query/gexp', gexpQueryGet);
router.get('/version', versionGet);
router.post('/version', versionGet);
router.get('/config', configGet);
router.get('/uid/uidmeta', uidMetaGet);

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
}

var installTsdbApi = function(app, incomingConfig) {
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

    if (config.logRequests) {

        // middleware specific to this router
        router.use(function timeLog(req, res, next) {
            console.log(new Date(Date.now())+': '+req.originalUrl);
            next();
        });
    }
    app.use('/api',router);
}

var setBackend = function(b) {
    backend = b;
};

module.exports = {
    backend: setBackend,
    install: installTsdbApi
}