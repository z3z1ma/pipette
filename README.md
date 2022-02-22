# pipette

The **easiest** way to get data into your database. Keep the power of code (no GUIs) without overbundling and overcoupling logic into (EL)T. Emit new line delimited json to the console and pipe it directly into a defined target your database. The philosophy is two-fold. 

One: We should separate the concern of loading data into a data source from the logic which is performing the extraction. This greatly simplifies the extractor, it decouples it from any particular loading implementation meaning no managing SqlAlchemy/JDBC/ODBC/etc. alongside data extraction related code, and allows "dumb" extractors (as low as 5 lines of code for example) to get the job done reliably because our loader is "smart".  

Two: that there are essentially 5 scenarios or ways in which we want to store data coming in from a source if we know that the transformation layer is handled by dbt. A source can be _anything_ from a Go script, Python script, Scala script, `curl` command, `cat` command on a log or json artifact; essentially anything that can emit JSON to the console (stdout). 

**Primary Concerns [JSON data]**

1. Unstructured: We want the data in its purest form leaving dbt to establish a semantic model. We leverage database types like JSONb for postgres, (the following are awaiting implementation) VARIANT for snowflake, JSON for MySQL, SUPER for redshift, or JSON for BigQuery as support is released.
2. Partial Destructuring: We want to extract some keys from the data and keep the rest in an unstructured column. This is useful for example when Salesforce has 2 consistent fields such as SystemModstamp and Id which can be used for `merge` strategies but the rest can be variable depending on the clients custom fieldset. 
3. Structured: We have an explicit schema based on expected top level keys from the incoming stream to be parsed out and kept. This is useful if our json object has many keys but we explicitly want just a few in a concrete schema.

**Secondary Concerns [Text data]**

4. Delimited: We have delimited data which is not JSON serialized but can be put into the source as an array-like object. Useful if we have to `cat` a CSV/TSV or some other uncontrolled source which is not JSON.
5. Text: The data is just new line separated textual data like logs. Immensely useful for easily ingesting log data which is a common use case.


## Usage

Note: pipette is native image compatible.

Consider the below config file, we will call it `salesforce.yml`

```yaml
version: 2

config:

  leads:

    rdsProdDatabase:
      adapter: postgres
      jdbc: jdbc:postgresql://${PGHOST}:${PGPORT}/analytics?user=eltServiceAcct&password=${PGPASSWORD}
      schema: salesforce
      target: leads
      storage-strategy: json  # <-- Unstructured
      ingestion-strategy: refresh

    ...more destinations for `leads`
```

The command below is an example of how to use Pipette with the above config defined.


```sh
# Standard Usage
emit salesforce leads | java -jar pipette.jar rdsProdDatabase --config-file salesforce.yml --target leads

# Example docker command [pending Dockerfile addition]
docker exec -i pipette rdsProdDatabase --config-file salesforce.yml --target leads < emit salesforce leads
```

Breaking our command down, we have this structure:

`java -jar pipette.jar [destinations] --config-file="[path/to/config]" --target="[target]"`

Using the structure above, we can correlate it directly to the YAML to better understand how pipette targets/destinations are selected:

```
version: 2
targets:
  [target]:
    [destination]:
      [config]
    [destination]:
      [config]

  [target]:
    [destination]:
      [config]
    [destination]:
      [config]
```

With adjustments to the YAML config example we showed initially, one can easily imagine a pipeline like the following which is keeping a staging database in sync with a production db with a single API pull. Or imagine an invocation routing a single source of truth into 2 completely different databases like Postgres and Snowflake.

```sh
# Multi destination command
emit salesforce leads | java -jar pipette.jar localDatabase rdsDevDatabase rdsProdDatabase --config-file salesforce.yml --target leads
```

### Strategies

So far we have seen the `refresh` ingestion strategy and the `json` storage strategy. What do these mean? Lets correlate each valid storage strategy to our initial 5 scenarios.

#### Storage Strategies

- `json`
  - unstructured
- `explicit-json-overflow`
  - partial destructuring
- `explicit`
  - structured
- `csv-array`
  - delimited text
- `text`
  - textual


#### Ingestion Strategies

- `merge`
  - compatible with `explicit-json-overflow` and `explicit`
  - requires one or more columns marked `pk` in config
- `refresh` 
  - compatible with all strategies
  - truncates and inserts into source refreshing it
- `append`
  - compatible with all strategies
  - source is treated as append only for all incoming data, useful for log/artifact pipelines or event data

Refresh and Append strategies are self explanatory. Merge is unique so lets look at an example. Consider the following config:

```yaml
version: 2

config:

  leads:

    ...previous configs above 

    rdsProdDatabase_leadSource:
      adapter: postgres
      jdbc: jdbc:postgresql://${PGHOST}:${PGPORT}/analytics?user=eltServiceAcct&password=${PGPASSWORD}
      schema: salesforce
      target: lead_source
      storage-strategy: explicit
      ingestion-strategy: merge
      columns:
        - name: Id
          pk: true  # <- A pk marked column is required for merge strat
        - name: Name
        - name: How_did_you_hear_about_us__c
        - name: LastActivityDate
          type: timestamp # <- Any valid db type can be used to cast a column. Useful for, as one example, enabling snapshots prior to a staging model in dbt

    ...more destinations for `leads`
```

With an explicit storage strategy, we explicitly define top level keys expected in the incoming new line delimited JSON to parse out. This enables us to define a pk which is an identifier that makes a record unique. Pipette will then handle merge strategies if directed whenever piping data into the data warehouse. Perhaps now we only emit last 3 days of activity if our emitter supports it:

`emit salesforce leads --recent=3 | java -jar pipette.jar rdsProdDatabase_leadSource --config-file salesforce.yml --target leads`

Pipette will update records accordingly. 


## Minimum working example

The below will pull bitcoin transactions into a database table.

```sh
echo '
version: 2
targets:
  # targets, somewhat akin to namespaces
  cryptoTransactions:
    # a target can have multiple destinations
    localDatabase:
      adapter: postgres
      # update the jdbc to your own
      jdbc: jdbc:postgresql://localhost:5432/test?user=alex&password=${PGPASSWORD}
      schema: bitcambio
      target: cypto_trans
      storage-strategy: json
      ingestion-strategy: append
' > cfg.yml

curl "https://api.coincap.io/v2/assets/bitcoin/history?interval=d1" | 
  java -jar pipette localDatabase \
    --config-file="cfg.yml" \
    --target="cryptoTransactions" \
    --jq=".data[]"
```

This follows the semantic of `[curl] [bash pipe] [pipette]` but curl could just as easily by `python get_bitcoin.py`. Lastly, notice the use of the flag `jq` in pipette. Pipette ships with an embedded jq which can be leveraged to transform json data which might not be new line delimited into new line delimited json. It can also efficiently alter the structure of the json completely. See more on its documentation [here](https://stedolan.github.io/jq/). You could also have piped the curl through jq yourself if you have it installed or through any tool which works with pipes. 

To wrap up the end to end pipeline, I'll put the T part of the ELT example below.

```sh
echo "
version: 2
sources:
  - name: bitcambio
    loader: pipette
    loaded_at_field: _etl_loaded_at  # All pipette loaded tables have this column
    freshness:
      {'warn_after': {'count': 24, 'period': 'hour'}}
    tables:
      - name: cypto_trans
        description: Bitcoin transactions extracted from api.coincap.io
" > models/stg_cypto_trans.yml

echo "
with source as (
  select * from {{source('bitcambio', 'cypto_trans')}}
)
select cast(data->>'priceUsd' as decimal) as bitcoin_price, 
       cast(data->>'date' as timestamp) as bitcoin_trade_date
from   source
" > models/stg_cypto_trans.sql

dbt run --select stg_cypto_trans+
```



**Full pipette pipeline with files already in place**

```
curl "https://api.coincap.io/v2/assets/bitcoin/history?interval=d1" | 
  java -jar pipette localDatabase \
    --config-file="cfg.yml" \
    --target="cryptoTransactions" \
    --jq=".data[]" &&
  dbt run --select stg_cypto_trans+
```
