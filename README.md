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

#### LongFrequencyAggregator
Count number of occurrences of some field's values(e.g. you have countryId as dimension and you want to count number of occurrences each country appears in input stream). This might help you when you would hold 2 datasources and would want to join between them(in RDBMS).

```json
{
  "type": "frequency",
  "name": "your_field_name_counts",
  "fieldName": "your_field_name",
  "maxNumberOfEntries": 100
}
```
you can also rollup over this field and this aggregate over non-aggreagted field(your_field_name) or partially aggregated field(your_field_name_agg)
as SQL:
```sql
   SELECT
      TIME_FLOOR(__time, 'PT5M') as t
    , FREQUENCY(your_field_name, 100) as freq
    , FREQUENCY(your_field_name_agg, 100) as agg_freq_after_rollup
  FROM data_source
```