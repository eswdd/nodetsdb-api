var request = require('supertest')
     , util = require('util')
   , assert = require('assert'),
     rewire = require('rewire');

function errorExpectedActual(msg, expected, actual) {
    var err = new Error(msg);
    err.expected = expected;
    err.actual = actual;
    err.showDiff = true;
    return err;
}
function errorActual(msg, actual) {
    var err = new Error(msg);
    err.actual = actual;
    err.showDiff = true;
    return err;
}
function assertArrayContainsOnly(arrayDesc, expected, actual) {
    if (actual.length != 4) {
        return errorExpectedActual('expected '+arrayDesc+' of length '+expected.length+', got ' + actual.length, expected.length, actual.length);
    }
    for (var i=0; i<expected.length; i++) {
        var lookFor = expected[i];
        if (actual.indexOf(lookFor) < 0) {
            return errorActual('expected '+arrayDesc+' to contain '+JSON.stringify(lookFor)+', but was ' + JSON.stringify(actual), actual);
        }
    }
}

describe('Inline FakeTSDB Internals', function () {
    var faketsdb;

    beforeEach(function () {
        var app = require('express')();
        faketsdb = rewire('../index');
        faketsdb.install(app, {logRequests:false});
    });

    afterEach(function () {
    });

    it('removes no results when applying a * filter', function() {
        var postBackendFiltering = faketsdb.__get__("postBackendFiltering");
        
        var wildcardFilter = {tagk:"host",type:"wildcard",filter:"*",group_by:false};

        var rawTimeSeries1 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host1", tagv_uid: "001"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type1", tagv_uid: "002"}
            },
            dps: [
                [1519926590, 1],
                [1519926591, 2],
                [1519926592, 3],
                [1519926593, 4]
            ]
        };
        var rawTimeSeries2 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host2", tagv_uid: "003"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type1", tagv_uid: "002"}
            },
            dps: [
                [1519926590, 5],
                [1519926591, 6],
                [1519926592, 7],
                [1519926593, 8]
            ]
        };


        var rawTimeSeries = [ rawTimeSeries1, rawTimeSeries2 ];
        postBackendFiltering(rawTimeSeries, [wildcardFilter]);
        assert.deepEqual([ rawTimeSeries1, rawTimeSeries2 ], rawTimeSeries);
    });

    it("removes results when applying a wildcard filter which doesn't match all timeseries", function() {
        var postBackendFiltering = faketsdb.__get__("postBackendFiltering");
        
        var wildcardFilter = {tagk:"host",type:"wildcard",filter:"host1*",group_by:false};

        var rawTimeSeries1 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host11", tagv_uid: "004"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type1", tagv_uid: "002"}
            },
            dps: [
                [1519926590, 1],
                [1519926591, 2],
                [1519926592, 3],
                [1519926593, 4]
            ]
        };
        var rawTimeSeries2 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host21", tagv_uid: "005"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type1", tagv_uid: "002"}
            },
            dps: [
                [1519926590, 5],
                [1519926591, 6],
                [1519926592, 7],
                [1519926593, 8]
            ]
        };
        
        var rawTimeSeries = [ rawTimeSeries1, rawTimeSeries2 ];
        postBackendFiltering(rawTimeSeries, [wildcardFilter]);
        assert.deepEqual([ rawTimeSeries1 ], rawTimeSeries);
    });

    it('removes no results when applying a literal_or filter which matches all timeseries', function() {
        var postBackendFiltering = faketsdb.__get__("postBackendFiltering");

        var wildcardFilter = {tagk:"host",type:"literal_or",filter:["host1","host2"],group_by:false};

        var rawTimeSeries1 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host1", tagv_uid: "001"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type1", tagv_uid: "002"}
            },
            dps: [
                [1519926590, 1],
                [1519926591, 2],
                [1519926592, 3],
                [1519926593, 4]
            ]
        };
        var rawTimeSeries2 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host2", tagv_uid: "003"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type1", tagv_uid: "002"}
            },
            dps: [
                [1519926590, 5],
                [1519926591, 6],
                [1519926592, 7],
                [1519926593, 8]
            ]
        };


        var rawTimeSeries = [ rawTimeSeries1, rawTimeSeries2 ];
        postBackendFiltering(rawTimeSeries, [wildcardFilter]);
        assert.deepEqual([ rawTimeSeries1, rawTimeSeries2 ], rawTimeSeries);
    });

    it('removes no results when applying an illiteral_or filter which matches all timeseries', function() {
        var postBackendFiltering = faketsdb.__get__("postBackendFiltering");

        var wildcardFilter = {tagk:"host",type:"illiteral_or",filter:["Host1","host2"],group_by:false};

        var rawTimeSeries1 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host1", tagv_uid: "001"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type1", tagv_uid: "002"}
            },
            dps: [
                [1519926590, 1],
                [1519926591, 2],
                [1519926592, 3],
                [1519926593, 4]
            ]
        };
        var rawTimeSeries2 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "Host2", tagv_uid: "003"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type1", tagv_uid: "002"}
            },
            dps: [
                [1519926590, 5],
                [1519926591, 6],
                [1519926592, 7],
                [1519926593, 8]
            ]
        };


        var rawTimeSeries = [ rawTimeSeries1, rawTimeSeries2 ];
        postBackendFiltering(rawTimeSeries, [wildcardFilter]);
        assert.deepEqual([ rawTimeSeries1, rawTimeSeries2 ], rawTimeSeries);
    });

    it('generates a complete set of tags when there are only group_by filters to apply', function() {
        var constructUniqueTagSetsFromRawResults = faketsdb.__get__("constructUniqueTagSetsFromRawResults");

        // we assume that filters have already been applied, so only really need to worry about grouping
        var literal_or = {tagk:"host",type:"literal_or",filter:["host11","host21"],group_by:true};
        var wildcard = {tagk:"type",type:"wildcard",filter:"type*",group_by:true};

        var rawTimeSeries1 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host11", tagv_uid: "004"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type1", tagv_uid: "002"}
            },
            dps: [
                [1519926590, 1],
                [1519926591, 2],
                [1519926592, 3],
                [1519926593, 4]
            ]
        };
        var rawTimeSeries2 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host21", tagv_uid: "005"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type1", tagv_uid: "002"}
            },
            dps: [
                [1519926590, 5],
                [1519926591, 6],
                [1519926592, 7],
                [1519926593, 8]
            ]
        };

        var rawTimeSeries3 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host11", tagv_uid: "004"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type2", tagv_uid: "006"}
            },
            dps: [
                [1519926590, 9],
                [1519926591, 10],
                [1519926592, 11],
                [1519926593, 12]
            ]
        };
        var rawTimeSeries4 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host21", tagv_uid: "005"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type2", tagv_uid: "006"}
            },
            dps: [
                [1519926590, 13],
                [1519926591, 14],
                [1519926592, 15],
                [1519926593, 16]
            ]
        };

        var expectedTagsets = [
            {host:"host11", type: "type1"},
            {host:"host21", type: "type1"},
            {host:"host11", type: "type2"},
            {host:"host21", type: "type2"}
        ];
        var tagsets = constructUniqueTagSetsFromRawResults([rawTimeSeries1, rawTimeSeries2, rawTimeSeries3, rawTimeSeries4], [literal_or, wildcard]);
        assert.deepEqual(tagsets, expectedTagsets);
    });

    it('generates an empty set of tags when there are no group_by filters to apply', function() {
        var constructUniqueTagSetsFromRawResults = faketsdb.__get__("constructUniqueTagSetsFromRawResults");

        // we assume that filters have already been applied, so only really need to worry about grouping
        var literal_or = {tagk:"host",type:"literal_or",filter:["host11","host21"],group_by:false};
        var wildcard = {tagk:"type",type:"wildcard",filter:"type*",group_by:false};

        var rawTimeSeries1 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host11", tagv_uid: "004"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type1", tagv_uid: "002"}
            },
            dps: [
                [1519926590, 1],
                [1519926591, 2],
                [1519926592, 3],
                [1519926593, 4]
            ]
        };
        var rawTimeSeries2 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host21", tagv_uid: "005"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type1", tagv_uid: "002"}
            },
            dps: [
                [1519926590, 5],
                [1519926591, 6],
                [1519926592, 7],
                [1519926593, 8]
            ]
        };

        var rawTimeSeries3 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host11", tagv_uid: "004"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type2", tagv_uid: "006"}
            },
            dps: [
                [1519926590, 9],
                [1519926591, 10],
                [1519926592, 11],
                [1519926593, 12]
            ]
        };
        var rawTimeSeries4 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host21", tagv_uid: "005"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type2", tagv_uid: "006"}
            },
            dps: [
                [1519926590, 13],
                [1519926591, 14],
                [1519926592, 15],
                [1519926593, 16]
            ]
        };

        var expectedTagsets = [{}];
        var tagsets = constructUniqueTagSetsFromRawResults([rawTimeSeries1, rawTimeSeries2, rawTimeSeries3, rawTimeSeries4], [literal_or, wildcard]);
        assert.deepEqual(tagsets, expectedTagsets);
    });

    it('generates a correct set of tags when there are mixed filters to apply', function() {
        var constructUniqueTagSetsFromRawResults = faketsdb.__get__("constructUniqueTagSetsFromRawResults");
        
        // we assume that filters have already been applied, so only really need to worry about grouping
        var literal_or = {tagk:"host",type:"literal_or",filter:["host11","host21"],group_by:true};
        var wildcard = {tagk:"type",type:"wildcard",filter:"type*",group_by:false};

        var rawTimeSeries1 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host11", tagv_uid: "004"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type1", tagv_uid: "002"}
            },
            dps: [
                [1519926590, 1],
                [1519926591, 2],
                [1519926592, 3],
                [1519926593, 4]
            ]
        };
        var rawTimeSeries2 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host21", tagv_uid: "005"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type1", tagv_uid: "002"}
            },
            dps: [
                [1519926590, 5],
                [1519926591, 6],
                [1519926592, 7],
                [1519926593, 8]
            ]
        };

        var rawTimeSeries3 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host11", tagv_uid: "004"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type2", tagv_uid: "006"}
            },
            dps: [
                [1519926590, 9],
                [1519926591, 10],
                [1519926592, 11],
                [1519926593, 12]
            ]
        };
        var rawTimeSeries4 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host21", tagv_uid: "005"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type2", tagv_uid: "006"}
            },
            dps: [
                [1519926590, 13],
                [1519926591, 14],
                [1519926592, 15],
                [1519926593, 16]
            ]
        };
        
        var expectedTagsets = [
            {host:"host11"},
            {host:"host21"}
        ];
        var tagsets = constructUniqueTagSetsFromRawResults([rawTimeSeries1, rawTimeSeries2, rawTimeSeries3, rawTimeSeries4], [literal_or, wildcard]);
        assert.deepEqual(tagsets, expectedTagsets);
    });

    it('correctly selects the single timeseries which match the given full tagset', function() {
        var rawTimeSeriesForTagSet = faketsdb.__get__("rawTimeSeriesForTagSet");

        var rawTimeSeries1 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host11", tagv_uid: "004"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type1", tagv_uid: "002"}
            },
            dps: [
                [1519926590, 1],
                [1519926591, 2],
                [1519926592, 3],
                [1519926593, 4]
            ]
        };
        var rawTimeSeries2 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host21", tagv_uid: "005"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type1", tagv_uid: "002"}
            },
            dps: [
                [1519926590, 5],
                [1519926591, 6],
                [1519926592, 7],
                [1519926593, 8]
            ]
        };

        var rawTimeSeries3 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host11", tagv_uid: "004"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type2", tagv_uid: "006"}
            },
            dps: [
                [1519926590, 9],
                [1519926591, 10],
                [1519926592, 11],
                [1519926593, 12]
            ]
        };
        var rawTimeSeries4 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host21", tagv_uid: "005"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type2", tagv_uid: "006"}
            },
            dps: [
                [1519926590, 13],
                [1519926591, 14],
                [1519926592, 15],
                [1519926593, 16]
            ]
        };
        var inputTs = [
            rawTimeSeries1,
            rawTimeSeries2,
            rawTimeSeries3,
            rawTimeSeries4
        ];
        
        var tagset1 = {
            host: "host11",
            type: "type1"
        };
        var tagset2 = {
            host: "host21",
            type: "type1"
        };
        var tagset3 = {
            host: "host11",
            type: "type2"
        };
        var tagset4 = {
            host: "host21",
            type: "type2"
        };
        
        assert.deepEqual(rawTimeSeriesForTagSet(inputTs, tagset1), [rawTimeSeries1]);
        assert.deepEqual(rawTimeSeriesForTagSet(inputTs, tagset2), [rawTimeSeries2]);
        assert.deepEqual(rawTimeSeriesForTagSet(inputTs, tagset3), [rawTimeSeries3]);
        assert.deepEqual(rawTimeSeriesForTagSet(inputTs, tagset4), [rawTimeSeries4]);
    });

    it('correctly selects the timeseries which match the given partial tagset', function() {
        var rawTimeSeriesForTagSet = faketsdb.__get__("rawTimeSeriesForTagSet");

        var rawTimeSeries1 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host11", tagv_uid: "004"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type1", tagv_uid: "002"}
            },
            dps: [
                [1519926590, 1],
                [1519926591, 2],
                [1519926592, 3],
                [1519926593, 4]
            ]
        };
        var rawTimeSeries2 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host21", tagv_uid: "005"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type1", tagv_uid: "002"}
            },
            dps: [
                [1519926590, 5],
                [1519926591, 6],
                [1519926592, 7],
                [1519926593, 8]
            ]
        };

        var rawTimeSeries3 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host11", tagv_uid: "004"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type2", tagv_uid: "006"}
            },
            dps: [
                [1519926590, 9],
                [1519926591, 10],
                [1519926592, 11],
                [1519926593, 12]
            ]
        };
        var rawTimeSeries4 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host21", tagv_uid: "005"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type2", tagv_uid: "006"}
            },
            dps: [
                [1519926590, 13],
                [1519926591, 14],
                [1519926592, 15],
                [1519926593, 16]
            ]
        };
        var inputTs = [
            rawTimeSeries1,
            rawTimeSeries2,
            rawTimeSeries3,
            rawTimeSeries4
        ];
        
        var tagset1 = {
            host: "host11"
        };
        var tagset2 = {
            type: "type2"
        };
        
        assert.deepEqual(rawTimeSeriesForTagSet(inputTs, tagset1), [rawTimeSeries1, rawTimeSeries3]);
        assert.deepEqual(rawTimeSeriesForTagSet(inputTs, tagset2), [rawTimeSeries3, rawTimeSeries4]);
    });

    it('correctly selects all the timeseries when given an empty tagset', function() {
        var rawTimeSeriesForTagSet = faketsdb.__get__("rawTimeSeriesForTagSet");

        var rawTimeSeries1 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host11", tagv_uid: "004"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type1", tagv_uid: "002"}
            },
            dps: [
                [1519926590, 1],
                [1519926591, 2],
                [1519926592, 3],
                [1519926593, 4]
            ]
        };
        var rawTimeSeries2 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host21", tagv_uid: "005"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type1", tagv_uid: "002"}
            },
            dps: [
                [1519926590, 5],
                [1519926591, 6],
                [1519926592, 7],
                [1519926593, 8]
            ]
        };

        var rawTimeSeries3 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host11", tagv_uid: "004"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type2", tagv_uid: "006"}
            },
            dps: [
                [1519926590, 9],
                [1519926591, 10],
                [1519926592, 11],
                [1519926593, 12]
            ]
        };
        var rawTimeSeries4 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host21", tagv_uid: "005"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type2", tagv_uid: "006"}
            },
            dps: [
                [1519926590, 13],
                [1519926591, 14],
                [1519926592, 15],
                [1519926593, 16]
            ]
        };
        var inputTs = [
            rawTimeSeries1,
            rawTimeSeries2,
            rawTimeSeries3,
            rawTimeSeries4
        ];
        
        var tagset1 = {};

        assert.deepEqual(rawTimeSeriesForTagSet(inputTs, tagset1), inputTs);
    });

    it('correctly combines a single time series where all points coincide with downsample boundary', function() {
        var combineTimeSeries = faketsdb.__get__("combineTimeSeries");

        var rawTimeSeries1 = {
            metric: "metric1",
            metric_uid: "001",
            tsuid: "001001004002002",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host11", tagv_uid: "004"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type1", tagv_uid: "002"}
            },
            dps: [
                [1519926560000, 1],
                [1519926570000, 2],
                [1519926580000, 3],
                [1519926590000, 4]
            ]
        };

        var combinedTimeSerie = combineTimeSeries([rawTimeSeries1], {}, ["host","type"], 1519926560000, 1519926590000, "metric1", "sum", true, false, true, false, true);

        var expectedCombinedTimeSerie =
            {
                metric: 'metric1',
                tags: {},
                tsuids: [
                    "001001004002002"
                ],
                aggregatedTags: ['host','type'],
                dps:
                    [
                        [1519926560000, 1],
                        [1519926570000, 2],
                        [1519926580000, 3],
                        [1519926590000, 4]
                    ]
            };

        assert.deepEqual(combinedTimeSerie, expectedCombinedTimeSerie);
    });

    it('correctly downsamples a set of points where they coincide with downsample boundary', function() {
        var downsampleSingleTimeSeriesPoints = faketsdb.__get__("downsampleSingleTimeSeriesPoints");

        var rawDps = [
            [1519926560000, 1],
            [1519926570000, 2],
            [1519926580000, 3],
            [1519926590000, 4]
        ];

        var dps = downsampleSingleTimeSeriesPoints(rawDps, 1519926560000, 1519926590000, 10000, "avg", "none");

        var expectedDps = [
            [1519926560000, 1],
            [1519926570000, 2],
            [1519926580000, 3],
            [1519926590000, 4]
        ];

        assert.deepEqual(dps, expectedDps);
    });

    it('correctly downsamples a set of points where they are offset from downsample boundary', function() {
        var downsampleSingleTimeSeriesPoints = faketsdb.__get__("downsampleSingleTimeSeriesPoints");

        var rawDps = [
            [1519926565000, 1],
            [1519926575000, 2],
            [1519926585000, 3],
            [1519926595000, 4]
        ];

        var dps = downsampleSingleTimeSeriesPoints(rawDps, 1519926560000, 1519926590000, 10000, "avg", "none");

        var expectedDps = [
            [1519926560000, 1],
            [1519926570000, 2],
            [1519926580000, 3],
            [1519926590000, 4]
        ];

        assert.deepEqual(dps, expectedDps);
    });

    it('correctly applies the fill in policy of none when downsampling a set of points where there are gaps', function() {
        var downsampleSingleTimeSeriesPoints = faketsdb.__get__("downsampleSingleTimeSeriesPoints");

        var rawDps = [
            [1519926565000, 1],
            [1519926585000, 3],
            [1519926595000, 4]
        ];

        var dps = downsampleSingleTimeSeriesPoints(rawDps, 1519926560000, 1519926590000, 10000, "avg", "none");

        var expectedDps = [
            [1519926560000, 1],
            [1519926580000, 3],
            [1519926590000, 4]
        ];

        assert.deepEqual(dps, expectedDps);
    });

    it('correctly applies the fill in policy of nan when downsampling a set of points where there are gaps', function() {
        var downsampleSingleTimeSeriesPoints = faketsdb.__get__("downsampleSingleTimeSeriesPoints");

        var rawDps = [
            [1519926565000, 1],
            [1519926585000, 3],
            [1519926595000, 4]
        ];

        var dps = downsampleSingleTimeSeriesPoints(rawDps, 1519926560000, 1519926590000, 10000, "avg", "nan");

        var expectedDps = [
            [1519926560000, 1],
            [1519926570000, NaN],
            [1519926580000, 3],
            [1519926590000, 4]
        ];

        assert.deepEqual(dps.length, 4);
        assert.deepEqual(dps[0], expectedDps[0]);
        assert.deepEqual(dps[1][0], expectedDps[1][0]);
        if (!isNaN(dps[1][1])) {
            assert.fail("Expect value for point 1 to be NaN");
        }
        assert.deepEqual(dps[2], expectedDps[2]);
        assert.deepEqual(dps[3], expectedDps[3]);
    });

    it('correctly applies the fill in policy of null when downsampling a set of points where there are gaps', function() {
        var downsampleSingleTimeSeriesPoints = faketsdb.__get__("downsampleSingleTimeSeriesPoints");

        var rawDps = [
            [1519926565000, 1],
            [1519926585000, 3],
            [1519926595000, 4]
        ];

        var dps = downsampleSingleTimeSeriesPoints(rawDps, 1519926560000, 1519926590000, 10000, "avg", "null");

        var expectedDps = [
            [1519926560000, 1],
            [1519926570000, null],
            [1519926580000, 3],
            [1519926590000, 4]
        ];

        assert.deepEqual(dps, expectedDps);
    });

    it('correctly applies the fill in policy of zero when downsampling a set of points where there are gaps', function() {
        var downsampleSingleTimeSeriesPoints = faketsdb.__get__("downsampleSingleTimeSeriesPoints");

        var rawDps = [
            [1519926565000, 1],
            [1519926585000, 3],
            [1519926595000, 4]
        ];

        var dps = downsampleSingleTimeSeriesPoints(rawDps, 1519926560000, 1519926590000, 10000, "avg", "zero");

        var expectedDps = [
            [1519926560000, 1],
            [1519926570000, 0],
            [1519926580000, 3],
            [1519926590000, 4]
        ];

        assert.deepEqual(dps, expectedDps);
    });

    it('correctly combines a single time series', function() {
        var combineTimeSeries = faketsdb.__get__("combineTimeSeries");

        var rawTimeSeries1 = {
            metric: "metric1",
            metric_uid: "001",
            tsuid: "001001004002002",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host11", tagv_uid: "004"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type1", tagv_uid: "002"}
            },
            dps: [
                [1519926565000, 1],
                [1519926575000, 2],
                [1519926585000, 3],
                [1519926595000, 4]
            ]
        };

        var combinedTimeSerie = combineTimeSeries([rawTimeSeries1], {}, ["host","type"], 1519926560000, 1519926600000, "metric1", "sum", false, false, true, false, true);

        var expectedCombinedTimeSerie =
            {
                metric: 'metric1',
                tags: {},
                tsuids: [
                    "001001004002002"
                ],
                aggregatedTags: ['host','type'],
                dps:
                    [
                        [1519926565, 1],
                        [1519926575, 2],
                        [1519926585, 3],
                        [1519926595, 4]
                    ]
            };

        assert.deepEqual(combinedTimeSerie, expectedCombinedTimeSerie);
    });

    it('correctly combines a single time series with ms precision', function() {
        var combineTimeSeries = faketsdb.__get__("combineTimeSeries");

        var rawTimeSeries1 = {
            metric: "metric1",
            metric_uid: "001",
            tsuid: "001001004002002",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host11", tagv_uid: "004"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type1", tagv_uid: "002"}
            },
            dps: [
                [1519926565000, 1],
                [1519926575000, 2],
                [1519926585000, 3],
                [1519926595000, 4]
            ]
        };

        var combinedTimeSerie = combineTimeSeries([rawTimeSeries1], {}, ["host","type"], 1519926560000, 1519926600000, "metric1", "sum", true, false, true, false, true);

        var expectedCombinedTimeSerie =
            {
                metric: 'metric1',
                tags: {},
                tsuids: [
                    "001001004002002"
                ],
                aggregatedTags: ['host','type'],
                dps:
                    [
                        [1519926565000, 1],
                        [1519926575000, 2],
                        [1519926585000, 3],
                        [1519926595000, 4]
                    ]
            };

        assert.deepEqual(combinedTimeSerie, expectedCombinedTimeSerie);
    });

    it('correctly combines a single time series where all points do not coincide with downsample boundary', function() {
        var combineTimeSeries = faketsdb.__get__("combineTimeSeries");

        var rawTimeSeries1 = {
            metric: "metric1",
            metric_uid: "001",
            tsuid: "001001004002002",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host11", tagv_uid: "004"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type1", tagv_uid: "002"}
            },
            dps: [
                [1519926565000, 1],
                [1519926595000, 4]
            ]
        };

        var combinedTimeSerie = combineTimeSeries([rawTimeSeries1], {}, ["host","type"], 1519926560000, 1519926600000, "metric1", "sum", true, false, true, false, true);

        var expectedCombinedTimeSerie =
            {
                metric: 'metric1',
                tags: {},
                tsuids: [
                    "001001004002002"
                ],
                aggregatedTags: ['host','type'],
                dps:
                    [
                        [1519926565000, 1],
                        [1519926595000, 4]
                    ]
            };

        assert.deepEqual(combinedTimeSerie, expectedCombinedTimeSerie);
    });

    it('correctly combines a 2 time series where no interpolation is required', function() {
        var combineTimeSeries = faketsdb.__get__("combineTimeSeries");

        var rawTimeSeries1 = {
            metric: "metric1",
            metric_uid: "001",
            tsuid: "001001004002002",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host11", tagv_uid: "004"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type1", tagv_uid: "002"}
            },
            dps: [
                [1519926565000, 1],
                [1519926595000, 4]
            ]
        };
        var rawTimeSeries2 = {
            metric: "metric1",
            metric_uid: "001",
            tsuid: "001001004002003",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host11", tagv_uid: "004"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type2", tagv_uid: "003"}
            },
            dps: [
                [1519926565000, 1],
                [1519926595000, 4]
            ]
        };

        var combinedTimeSerie = combineTimeSeries([rawTimeSeries1, rawTimeSeries2], {}, ["host","type"], 1519926560000, 1519926600000, "metric1", "sum", true, false, true, false, true);

        var expectedCombinedTimeSerie =
            {
                metric: 'metric1',
                tags: {},
                tsuids: [
                    "001001004002002",
                    "001001004002003"
                ],
                aggregatedTags: ['host','type'],
                dps:
                    [
                        [1519926565000, 2],
                        [1519926595000, 8]
                    ]
            };

        assert.deepEqual(combinedTimeSerie, expectedCombinedTimeSerie);
    });

    it('correctly combines a 2 time series where interpolation is required', function() {
        var combineTimeSeries = faketsdb.__get__("combineTimeSeries");

        var rawTimeSeries1 = {
            metric: "metric1",
            metric_uid: "001",
            tsuid: "001001004002002",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host11", tagv_uid: "004"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type1", tagv_uid: "002"}
            },
            dps: [
                [1519926565000, 1],
                [1519926595000, 4]
            ]
        };
        var rawTimeSeries2 = {
            metric: "metric1",
            metric_uid: "001",
            tsuid: "001001004002003",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host11", tagv_uid: "004"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type2", tagv_uid: "003"}
            },
            dps: [
                [1519926575000, 2],
                [1519926585000, 3]
            ]
        };

        var combinedTimeSerie = combineTimeSeries([rawTimeSeries1, rawTimeSeries2], {}, ["host","type"], 1519926560000, 1519926600000, "metric1", "sum", true, false, true, false, true);

        var expectedCombinedTimeSerie =
            {
                metric: 'metric1',
                tags: {},
                tsuids: [
                    "001001004002002",
                    "001001004002003"
                ],
                aggregatedTags: ['host','type'],
                dps:
                    [
                        [1519926565000, 1],
                        [1519926575000, 4],
                        [1519926585000, 6],
                        [1519926595000, 4]
                    ]
            };

        assert.deepEqual(combinedTimeSerie, expectedCombinedTimeSerie);
    });

/*
A:
[1519926565000, 770],
[1519926595000, 350]
B:
[1519926585000, 40],
[1519926625000, 630]
C:
[1519926590000, 680],
[1519926605000, 770],
[1519926610000, 0],
[1519926640000, 800]
D:
[1519926620000, 660],
[1519926640000, 800]
E:
[1519926597000, 680]

T               |   A   |   B   |   C   |   D   |   E   |   avg   |
1519926565000      770     none    none    none    none     770
1519926585000     (490)     40     none    none    none     265
1519926590000     (420)  (113.75)   680     none    none    404.5833333333333
1519926595000      350   (187.5)  (707)    none    none     415.8333333333333
1519926597000      none   (217)   (722)    none    680      539.6666666666666
1519926605000      none   (335)    770     none    none     552.5
1519926610000      none  (408.75)   0      none    none     204.375
1519926620000      none  (556.25) (266.67) 660     none     494.3055555555555
1519926625000      none    630    (400)   (695)    none     575
1519926640000      none    none    800     800     none     800



 */

    it('correctly combines a multiple time series where interpolation is required (complex example from docs)', function() {
        var combineTimeSeries = faketsdb.__get__("combineTimeSeries");

        var rawTimeSeries1 = {
            metric: "metric1",
            metric_uid: "001",
            tsuid: "001001004002002",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host11", tagv_uid: "004"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type1", tagv_uid: "002"}
            },
            dps: [
                [1519926565000, 770],
                [1519926595000, 350]
            ]
        };
        var rawTimeSeries2 = {
            metric: "metric1",
            metric_uid: "001",
            tsuid: "001001004002003",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host11", tagv_uid: "004"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type2", tagv_uid: "003"}
            },
            dps: [
                [1519926585000, 40],
                [1519926625000, 630]
            ]
        };
        var rawTimeSeries3 = {
            metric: "metric1",
            metric_uid: "001",
            tsuid: "001001004002004",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host11", tagv_uid: "004"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type3", tagv_uid: "004"}
            },
            dps: [
                [1519926590000, 680],
                [1519926605000, 770],
                [1519926610000, 0],
                [1519926640000, 800]
            ]
        };
        var rawTimeSeries4 = {
            metric: "metric1",
            metric_uid: "001",
            tsuid: "001001004002005",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host11", tagv_uid: "004"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type4", tagv_uid: "005"}
            },
            dps: [
                [1519926620000, 660],
                [1519926640000, 800]
            ]
        };
        var rawTimeSeries5 = {
            metric: "metric1",
            metric_uid: "001",
            tsuid: "001001004002006",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host11", tagv_uid: "004"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type5", tagv_uid: "006"}
            },
            dps: [
                [1519926597000, 680]
            ]
        };

        var combinedTimeSerie = combineTimeSeries([rawTimeSeries1, rawTimeSeries2, rawTimeSeries3, rawTimeSeries4, rawTimeSeries5], {}, ["host","type"], 1519926560000, 1519926600000, "metric1", "avg", true, false, true, false, true);

        var expectedCombinedTimeSerie =
            {
                metric: 'metric1',
                tags: {},
                tsuids: [
                    "001001004002002",
                    "001001004002003",
                    "001001004002004",
                    "001001004002005",
                    "001001004002006"
                ],
                aggregatedTags: ['host','type'],
                dps:
                    [
                        [
                            1519926565000,
                            770
                        ],
                        [
                            1519926585000,
                            265
                        ],
                        [
                            1519926590000,
                            404.5833333333333
                        ],
                        [
                            1519926595000,
                            415.8333333333333
                        ],
                        [
                            1519926597000,
                            539.6666666666666
                        ],
                        [
                            1519926605000,
                            552.5
                        ],
                        [
                            1519926610000,
                            204.375
                        ],
                        [
                            1519926620000,
                            494.3055555555555
                        ],
                        [
                            1519926625000,
                            575
                        ],
                        [
                            1519926640000,
                            800
                        ]
                    ]
            };

        assert.deepEqual(combinedTimeSerie, expectedCombinedTimeSerie);
    });

    it('correctly rates a single time series', function() {
        var combineTimeSeries = faketsdb.__get__("combineTimeSeries");

        var rawTimeSeries1 = {
            metric: "metric1",
            metric_uid: "001",
            tsuid: "001001004002002",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host11", tagv_uid: "004"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type1", tagv_uid: "002"}
            },
            dps: [
                [1519926565000, 1],
                [1519926575000, 2],
                [1519926585000, 3],
                [1519926595000, 4]
            ]
        };

        var combinedTimeSerie = combineTimeSeries([rawTimeSeries1], {}, ["host","type"], 1519926560000, 1519926600000, "metric1", "sum", false, true, true, false, true);

        var expectedCombinedTimeSerie =
            {
                metric: 'metric1',
                tags: {},
                tsuids: [
                    "001001004002002"
                ],
                aggregatedTags: ['host','type'],
                dps:
                    [
                        [1519926575, 0.1],
                        [1519926585, 0.1],
                        [1519926595, 0.1]
                    ]
            };

        assert.deepEqual(combinedTimeSerie, expectedCombinedTimeSerie);
    });

    it('correctly rates a single time series with ms precision', function() {
        var combineTimeSeries = faketsdb.__get__("combineTimeSeries");

        var rawTimeSeries1 = {
            metric: "metric1",
            metric_uid: "001",
            tsuid: "001001004002002",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host11", tagv_uid: "004"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type1", tagv_uid: "002"}
            },
            dps: [
                [1519926565000, 1],
                [1519926575000, 2],
                [1519926585000, 3],
                [1519926595000, 4]
            ]
        };

        var combinedTimeSerie = combineTimeSeries([rawTimeSeries1], {}, ["host","type"], 1519926560000, 1519926600000, "metric1", "sum", true, true, true, false, true);

        var expectedCombinedTimeSerie =
            {
                metric: 'metric1',
                tags: {},
                tsuids: [
                    "001001004002002"
                ],
                aggregatedTags: ['host','type'],
                dps:
                    [
                        [1519926575000, 0.1],
                        [1519926585000, 0.1],
                        [1519926595000, 0.1]
                    ]
            };

        assert.deepEqual(combinedTimeSerie, expectedCombinedTimeSerie);
    });

    it('correctly rates 2 time series where no interpolation is required', function() {
        var combineTimeSeries = faketsdb.__get__("combineTimeSeries");

        var rawTimeSeries1 = {
            metric: "metric1",
            metric_uid: "001",
            tsuid: "001001004002002",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host11", tagv_uid: "004"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type1", tagv_uid: "002"}
            },
            dps: [
                [1519926565000, 1],
                [1519926595000, 4]
            ]
        };
        var rawTimeSeries2 = {
            metric: "metric1",
            metric_uid: "001",
            tsuid: "001001004002003",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host11", tagv_uid: "004"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type2", tagv_uid: "003"}
            },
            dps: [
                [1519926565000, 1],
                [1519926595000, 4]
            ]
        };

        var combinedTimeSerie = combineTimeSeries([rawTimeSeries1, rawTimeSeries2], {}, ["host","type"], 1519926560000, 1519926600000, "metric1", "sum", true, true, true, false, true);

        var expectedCombinedTimeSerie =
            {
                metric: 'metric1',
                tags: {},
                tsuids: [
                    "001001004002002",
                    "001001004002003"
                ],
                aggregatedTags: ['host','type'],
                dps:
                    [
                        [1519926595000, 0.2]
                    ]
            };

        assert.deepEqual(combinedTimeSerie, expectedCombinedTimeSerie);
    });

    it('correctly rates 2 time series where interpolation is required', function() {
        var combineTimeSeries = faketsdb.__get__("combineTimeSeries");

        var rawTimeSeries1 = {
            metric: "metric1",
            metric_uid: "001",
            tsuid: "001001004002002",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host11", tagv_uid: "004"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type1", tagv_uid: "002"}
            },
            dps: [
                [1519926565000, 1],
                [1519926595000, 4]
            ]
        };
        var rawTimeSeries2 = {
            metric: "metric1",
            metric_uid: "001",
            tsuid: "001001004002003",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host11", tagv_uid: "004"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type2", tagv_uid: "003"}
            },
            dps: [
                [1519926575000, 2],
                [1519926585000, 3]
            ]
        };

        var combinedTimeSerie = combineTimeSeries([rawTimeSeries1, rawTimeSeries2], {}, ["host","type"], 1519926560000, 1519926600000, "metric1", "sum", true, true, true, false, true);

        var expectedCombinedTimeSerie =
            {
                metric: 'metric1',
                tags: {},
                tsuids: [
                    "001001004002002",
                    "001001004002003"
                ],
                aggregatedTags: ['host','type'],
                dps:
                    [
                        [1519926575000, 0.3],
                        [1519926585000, 0.2],
                        [1519926595000, -0.2]
                    ]
            };

        assert.deepEqual(combinedTimeSerie, expectedCombinedTimeSerie);
    });

    it('correctly parses a simple metric query for search/lookup', function(done) {
        var parseSearchLookupQuery = faketsdb.__get__("parseSearchLookupQuery");
        parseSearchLookupQuery("some.metric", function(parsedQuery, err) {
            assert.equal(err, null);
            assert.deepEqual(parsedQuery, {metric:"some.metric",tags:[]});
            done();
        })
    });

    it('correctly parses a simple wildcard tag key query for search/lookup', function(done) {
        var parseSearchLookupQuery = faketsdb.__get__("parseSearchLookupQuery");
        parseSearchLookupQuery("{tagk=*}", function(parsedQuery, err) {
            assert.equal(err, null);
            assert.deepEqual(parsedQuery, {metric:null,tags:[{key:"tagk",value:"*"}]});
            done();
        })
    });

    it('correctly parses a simple wildcard tag key query for search/lookup', function(done) {
        var parseSearchLookupQuery = faketsdb.__get__("parseSearchLookupQuery");
        parseSearchLookupQuery("{*=tagv}", function(parsedQuery, err) {
            assert.equal(err, null);
            assert.deepEqual(parsedQuery, {metric:null,tags:[{key:"*",value:"tagv"}]});
            done();
        })
    });

    it('correctly parses a complex query for search/lookup', function(done) {
        var parseSearchLookupQuery = faketsdb.__get__("parseSearchLookupQuery");
        parseSearchLookupQuery("some.metric{key1=*,key2=value1,key2=value2,*=value3}", function(parsedQuery, err) {
            assert.equal(err, null);
            assert.deepEqual(parsedQuery, {metric:"some.metric",tags:[{key:"key1",value:"*"},{key:"key2",value:"value1"},{key:"key2",value:"value2"},{key:"*",value:"value3"}]});
            done();
        })
    });

    it('correctly requires a valid query for search/lookup', function(done) {
        var parseSearchLookupQuery = faketsdb.__get__("parseSearchLookupQuery");
        parseSearchLookupQuery("", function(parsedQuery, err) {
            assert.equal(err, "You must specify at least one metric, tagk or tagv in your query");
            assert.equal(parsedQuery, null);
            done();
        })
    });

    it('correctly requires a query for search/lookup', function(done) {
        var parseSearchLookupQuery = faketsdb.__get__("parseSearchLookupQuery");
        parseSearchLookupQuery(null, function(parsedQuery, err) {
            assert.equal(err, "You must specify a query");
            assert.equal(parsedQuery, null);
            done();
        })
    });
    
    
});