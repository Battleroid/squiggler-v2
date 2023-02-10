# Squiggler Deuce

Squiggler is our solution for the automation of creation and migration of indices to the rollover pattern. It's not perfect, but it'll do.

## Warning

This project was originally for ES 6/7.x, and as such has some odd decisions as new features came and went within ES and Kibana. Some things are bolted on, and large portions of this project are no longer used. It has its issues, but for a time it worked great, still more or less does.

## Process

Squiggler does the following (examples follow the fictitious `some-new-index`):

0. Documents are pushed to a new index that has no rollover alias associated via Logstash, in this case it's `some-new-index`. The index will be perceived as new if:
    * There's no alias for `some-new-index` to a rollover index. In this case it really is new.
    * Even if the alias previously existed, if the index it pointed to was closed (due to age) or deleted the alias will be deleted along with it.
1. Looks for new indices; `some-new-index` is found.
    * Filters the new indices found against our exclusion parameters.
    * Filters out anything that is actively being used in a current alias (e.g. `cat => cat-2018.08.14-000001` is left alone).
    * Filters out anything that _looks_ like it used to be part of a rollover index (by regex, e.g. `dog-2018.08.14-000299` is left alone).
    * Filters out anything that appears to be date indexed, such as monthly/daily/hourly patterns like `dog-2018.08`, `cat-2018.08.14`.
    * Filters out anything with a preceding dot `.`, like `.kibana`, `.tasks`, etc. These are generally used internally by Elasticsearch or special in their purpose.
2. For each index, a job is spawned that ...
    1. Creates [date math](https://www.elastic.co/guide/en/elasticsearch/reference/current/date-math-index-names.html) indices for new patterns, in the form of `<some-new-index-{now/d}-000001>`.
    2. Starts a reindex job from the original index `some-new-index` to the date math index of `some-new-index-{now/d}-000001`.
    3. After the reindex is completed, an atomic request to the aliases API is made that removes the old index `some-new-index` first, and adds a new alias of `some-new-index` to the rollover index `some-new-index-2018.08.14-000001`.

3. Looks for rollable aliases. Submits rollover request based on the `max_age`, `max_size` parameters given. The defaults will roll aliases at the 30 day mark and when they reach a 10GiB per shard mark.
4. (If enabled) Looks at Kibana's API for index patterns, grabs the list of rollover aliases. Filtering the aliases of anything that is already in Kibana.
5. (If enabled) Then creates individual patterns via Kibana's API, optionally notifying a Slack webhook with the list of aliases.
    * **Note:** _technically new_ rollover indices that are created that already exist as an index pattern in the remote/local Kibana will not re-notify until the index pattern is missing in the destination. This is different than the older Squiggler behavior where it would notify every time a new rollover index was made which could duplicate anytime an index was closed, remade and recreated as a rollover index.

All actions can be selectively enabled or disabled, with unique exclusion parameters at each step in case you want to separate the duties to different cronjobs.

## Caveats

Squiggler is primarily for _logging_ data. It is not ideal for _production_ data due to how it moves data from the old index to the new rollover index. Since there's no nice way within Elasticsearch to cut over data to a rollover pattern, there will likely be some documents lost when reindexing from the original index to the new. This is relatively fine for logging data where it's acceptable we might not have everything nor need everything.

Rollover indices are greatly useful for controlling the number of shards and the size of the indices. This works great for logging data due to how frequent and infrequent some indices are over others. Indices that only receive ~50GiB of data per month do not need a new index for every hour every few days they receive new data. This leads to a build of incredibly small indices with so many shards. Compound this issue with _many_ unique indices with the same issue and you will run out of heap space to store the metadata of them all.

This is not acceptable for production data where you need everything. Therefore, for production type use, it will be preferable to use timestamped indices based on the month/day/hour, potentially reindexing as a job every so often if you want to control the number of indices instead.

---

The above guidance is no longer an issue with the advent of some modifications to the Logstash output to establish ILM indices directly. As well as the introduction of data streams. Squiggler can still be used to create and dump index patterns with Kibana with these advances.

## Usage

```
usage: squiggler.py [-h] [--for-real] [--verbose] config

positional arguments:
  config

optional arguments:
  -h, --help  show this help message and exit
  --for-real  turn off dry run
  --verbose   verbose logging
```

## Configuration Gotchas

* If you're specifying `remote_kibana`, `elasticsearch` must be filled out (duh) so it can find and setup remote index patterns properly.
* During parsing of the config on startup, if `elasticsearch`, `local_kibama` or `remote_kibana` are not accessible on first startup (if they're used) then they will raise an exception and Squiggler will fail.
* Exceptions during individual actions should not affect other actions.

## Examples

See [sample.yaml](sample.yaml) for sample configuration and default field values.

Without verbose logging you get just a summary of the actions taken and their durations. The idea being this is what would be logged on output and ingested to Elasticsearch for aggregations. The timings and a status indicator are also pushed to Statsd (if specified in outputs).

```json
{
  "action_durations": {
    "create_remote_kibana_patterns": 0.8102409839630127,
    "create_rollover_indices": 0.7224798202514648,
    "update_rollover_aliases": 0.5127968788146973
  },
  "event": "finished",
  "level": "info",
  "logger": "squiggler",
  "timestamp": "2018-08-17T18:16:59.584366Z",
  "total_duration": 2.045517683029175
}
