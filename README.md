## Druid module for implementing map like aggregations 

### Build

To build the extension, run `mvn package` and you'll get a file in `target` like this:

```
[INFO] Building tar: /src/druid-complexaggs-extension/target/druid-complexaggs-extension-24.0.1.0-SNAPSHOT-bin.tar.gz
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time: 4.841 s
[INFO] Finished at: 2045-11-04T20:00:53Z
[INFO] Final Memory: 21M/402M
[INFO] ------------------------------------------------------------------------
```

Unpack the tar.gz and you'll find a directory named `druid-complexaggs-extension` inside it:

```
$ tar xzf target/druid-complexaggs-extension-24.0.1.0-SNAPSHOT-bin.tar.gz
$ ls druid-complexaggs-extension-24.0.1.0-SNAPSHOT/
LICENSE                  README.md                druid-complexaggs-extension/
```

### Install

To install the extension:

1. Copy `druid-complexaggs-extension` into your Druid `extensions` directory.
2. Edit `conf/_common/common.runtime.properties` to add `"druid-complexaggs-extension"` to `druid.extensions.loadList`. (Edit `conf-quickstart/_common/common.runtime.properties` too if you are using the quickstart config.)
It should look like: `druid.extensions.loadList=["druid-complexaggs-extension"]`. There may be a few other extensions there
too.
3. Restart Druid.

### Use

#### ExampleExtractionFn
To use the example extractionFn, call it like a normal extractionFn with type "example", e.g. in a
topN. It returns the first "length" characters of each value.

```json
{
  "queryType": "topN",
  "dataSource": "wikiticker",
  "intervals": [
    "2016-06-27/2016-06-28"
  ],
  "granularity": "all",
  "dimension": {
    "type": "extraction",
    "dimension": "page",
    "outputName": "page",
    "extractionFn": {
      "type": "example",
      "length": 5
    }
  },
  "metric": "edits",
  "threshold": 25,
  "aggregations": [
    {
      "type": "longSum",
      "name": "edits",
      "fieldName": "count"
    }
  ]
}
```

#### ExampleAggregator
To use the example aggregator, use the type "exampleSum". It does the same thing as the built-in
"doubleSum" aggregator.


#### ExampleByteBufferInputRowParser

The `ExampleByteBufferInputRowParser` illustrates how an extension can to do a custom transformation of binary input 
data during indexing. This example extension translates rot13 encoded base64 binary data into Strings, which can then
be transformed into a `Map` by any `ParseSpec` that that implements a string parser, e.g. `json`, `csv`, `tsv`, and so 
on.

```json
{
  "type": "kafka",
  "dataSchema": {
    "dataSource": "example-dataset",
    "parser": {
      "type": "exampleParser",
      "parseSpec": {
        "format": "json",
        "timestampSpec": {
          "column": "timestamp",
          "format": "auto"
        },
        "dimensionsSpec": {
          "dimensions": []
        }
      }
    },
    "metricsSpec": [
      {
        "name": "count",
        "type": "count"
      }
    ],
    "granularitySpec": {
      "type": "uniform",
      "segmentGranularity": "HOUR",
      "queryGranularity": "NONE"
    }
  },
  "tuningConfig": {
    "type": "kafka",
    "maxRowsPerSegment": 5000000
  },
  "ioConfig": {
    "topic": "rot13base64json",
    "consumerProperties": {
      "bootstrap.servers": "localhost:9092"
    },
    "taskCount": 1,
    "replicas": 1,
    "taskDuration": "PT1H"
  }
}
```