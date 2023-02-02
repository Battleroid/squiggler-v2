# vim: set cc=120:
import argparse
import logging
import logging.config
import os
import re
import socket
import time
import warnings
from typing import Tuple
from collections import namedtuple
from contextlib import contextmanager
from datetime import datetime
from enum import IntEnum
from multiprocessing import cpu_count
from pathlib import Path
from random import choice

import requests
import structlog
from cerberus import Validator
from elasticsearch import Elasticsearch
from gevent.threadpool import ThreadPool
from requests.exceptions import ConnectionError
from ruamel.yaml import YAML
from urllib3.util import parse_url

# Shut up, just shut up please, these break JSON logging, I don't care
warnings.filterwarnings("ignore")

# Setup logging, disable ES logger
logging.config.dictConfig(
    {
        "version": 1,
        "disable_existing_loggers": True,
        "formatters": {"default": {"format": "%(message)s"}},
        "handlers": {
            "null": {"class": "logging.NullHandler"},
            "default": {"class": "logging.StreamHandler", "formatter": "default"},
        },
        "loggers": {
            "elasticsearch": {"handlers": ["null"], "propagate": False},
            "": {"handlers": ["default"], "level": logging.INFO},
        },
    }
)

structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer(sort_keys=True),
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

log = structlog.wrap_logger(logging.getLogger("squiggler"))


class ElasticsearchAuthException(Exception):
    """
    Could not authenticate with Elasticsearch cluster.
    """

    def __init__(self, host: str):
        self.host = host


class ElasticsearchClusterStatus(Exception):
    """
    Status is not desirable.
    """

    def __init__(self, current_status: str, desired_status: str):
        self.current_status = current_status
        self.desired_status = desired_status

    def __repr__(self):
        return f"<ElasticsearchClusterStatus {self.current_status} vs {self.desired_status}>"


class KibanaAPIError(Exception):
    """
    Could not contact API.
    """

    def __init__(self, host: str, message: str = ""):
        self.host = host
        self.message = message


class InvalidExcludeMethodException(Exception):
    """
    Exclusion method is not valid.
    """

    def __init__(self, method: str):
        self.method = method


class Status(IntEnum):
    """
    Enum representation of cluster status for comparisons.
    """

    GREEN = 3
    YELLOW = 2
    RED = 1


class Slack:
    """
    Slack notifier.
    """

    def __init__(
        self,
        webhook: str,
        message: str = None,
        emojis: str = None,
        notify_on_local: bool = False,
        notify_on_remote: bool = True,
        dry_run: bool = True,
    ):
        self._log = log.new(dry_run="ON" if dry_run else "OFF")
        self.webhook = webhook
        self.message = message or "{emoji} New pattern(s) made: {patterns}"
        self.emojis = (emojis or "🚀,😄,💪,🙏,🙌,🙈,😂,😎,😱,😈").split(",")
        self.notify_on_local = notify_on_local
        self.notify_on_remote = notify_on_remote
        self.dry_run = dry_run
        self._log.debug(
            "created_slack_notifier",
            notify_on_local=self.notify_on_local,
            notify_on_remote=self.notify_on_remote,
        )

    @classmethod
    def from_config(cls, config: dict, dry_run: bool = True) -> "Slack":
        """
        Create Slack obj from config.
        """
        webhook = config["webhook"]
        message = config.get("message")
        emojis = config.get("emojis")
        notify_on_local = config.get("notify_on_local", False)
        notify_on_remote = config.get("notify_on_remote", True)
        return Slack(
            webhook, message, emojis, notify_on_local, notify_on_remote, dry_run
        )

    def notify(self, patterns: dict, host: str):
        """
        Send message to webhook.
        """
        log = self._log.bind(func="notify")
        url_patterns = [
            f"<{host}/app/kibana#/discover?_a=(index:'{d}')|{p}>"
            for p, d in patterns.items()
            if d is not None and d is not 'None'
        ]
        if not url_patterns:
            log.debug("no_patterns_for_slack")
            return
        message = self.message.format(
            emoji=choice(self.emojis), patterns=", ".join(url_patterns)
        )
        payload = {"text": message}
        if not self.dry_run:
            requests.post(self.webhook, json=payload, timeout=60)
        log.debug("notified_slack", payload=payload)


class Statsd:
    """
    For shipping metrics to statsd.
    """

    def __init__(self, host: str, prefix: str, name: str, dry_run: bool = True):
        self._log = log.new(dry_run="ON" if dry_run else "OFF")
        _host = parse_url(host)
        self.host = _host.host
        self.port = _host.port
        self.addr = (self.host, self.port)
        self.prefix = prefix
        self.name = name
        self.dry_run = dry_run

    @classmethod
    def from_config(cls, config: dict, dry_run: bool = True) -> "Statsd":
        """
        Create Statsd obj from config.
        """
        host = config["host"]
        prefix = config.get("prefix", "squiggler")
        name = config["name"]
        return Statsd(host, prefix, name, dry_run)

    # TODO: not sure if we need this, plus I think this is the graphite format, not statsd, CBF to check
    def notify(self, metrics):
        """
        Send metrics to statsd, shouldn't have a lot, so not even gonna bother with chunking like the previous
        Squiggler did.
        """
        log = self._log.bind(metrics=metrics)
        ts = datetime.now().strftime("%s")
        message = "\n".join(
            [" ".join([f"{self.prefix}.{m[0]}", str(m[1]), ts]) for m in metrics]
        )
        try:
            s = socket.socket()
            s.connect(self.addr)
            s.sendall(message.encode("utf-8"))
        except Exception as e:
            log.error(f"graphite_send_error", message=message, exc_info=True)
        finally:
            s.close()


class Excluder:
    """
    Determines if an index is excluded or not based on type and patterns.
    Providing 'none' excludes nothing.

    >>> Excluder('prefix', ['backfill-'])('backfill-something')
    True
    >>> Excluder('prefix', ['notamatch-'])('some-index')
    False
    """

    def __init__(self, method: str = "none", patterns: list = None, name: str = None):
        if method.casefold() not in ["prefix", "suffix", "regex", "none"]:
            raise InvalidExcludeMethodException(method)

        self.method = method.casefold()

        # Setup patterns, compile regexes if applicable
        if self.method != "none":
            if not patterns:
                patterns = []
            self.patterns = [
                re.compile(p, re.IGNORECASE) if self.method == "regex" else p
                for p in patterns
            ]

        if not name:
            name = "excluder"
        self._log = log.new(
            exclude_method=method, exclude_patterns=patterns, context=name
        )
        self._log.debug("setup_excluder")

    @classmethod
    def from_config(cls, config: dict, name: str = None) -> "Excluder":
        """
        Create Excluder obj from config.
        """
        method = config.get("exclude_method", "none")
        patterns = config.get("exclude_patterns", [])
        return Excluder(method, patterns, name)

    def __call__(self, index) -> bool:
        """
        Return True if index matches exclusion params, False otherwise.
        """
        is_excluded = False
        if self.method == "regex":
            is_excluded = any([p.match(index) for p in self.patterns])

        if self.method == "prefix":
            is_excluded = any([index.startswith(p) for p in self.patterns])

        if self.method == "suffix":
            is_excluded = any([index.endswith(p) for p in self.patterns])

        self._log.debug("is_excluded", index=index, is_excluded=is_excluded)
        return is_excluded


class ES:
    """
    Elasticsearch object responsible for context management, various reindex
    and rollover related interaction.
    """

    # TODO: interactions with es need to hangle breakages reasonably ok, it'll either be ugly with many try/except
    # or we'll have to be lenient about the statuses we ignore, as well as how long we can wait for a timeout

    def __init__(
        self, host: str, auth: tuple, rollover: dict = None, dry_run: bool = True
    ):
        _host = parse_url(host)
        if not _host.port:
            raise SystemExit(
                "Host must have a port component, e.g. https://localhost:9200!"
            )
        self._log = log.new(cluster=_host.host, dry_run="ON" if dry_run else "OFF")
        self.dry_run = dry_run

        # Elasticsearch settings/obj
        self.host = host
        self.auth = auth
        self.es = Elasticsearch(
            host,
            verify_certs=True,
            basic_auth=auth,
            request_timeout=30,
            retry_on_timeout=True,
            max_retries=5,
        )

        if not self.es.ping():
            raise ElasticsearchAuthException(host)

        self.cluster = self.es.info()["cluster_name"]
        self._log.debug("setup_es", cluster=self.cluster)

        # Rollover settings
        if not rollover:
            rollover = {}
        self.rollover_max_age = rollover.get("max_age", "30d")
        self.rollover_target_shard_size = rollover.get("target_shard_size", "10gb")
        self.rollover_date_math_pattern = rollover.get(
            "date_math_pattern", "<{index}-{{now/d}}-000001>"
        )
        self.rollover_date_indexed_regex = re.compile(
            rollover.get("date_indexed_regex", ".*\-\d{4}(?:\.\d{2}){1,}$")
        )

        date_math_regex = rollover.get(
            "date_math_regex", ".*\-\d{4}\.\d{2}\.\d{2}\-\d{6}$"
        )
        date_math_regex_removal = rollover.get(
            "date_math_regex_removal", "\-\d{4}\.\d{2}\.\d{2}\-\d{6}$"
        )
        self.rollover_date_math_regex = re.compile(date_math_regex)
        self.rollover_date_math_regex_removal = re.compile(date_math_regex_removal)
        self.ilm_rollover_enabled = rollover.get("ilm_rollover_enabled", False)
        self.data_streams_enabled = rollover.get("data_streams_enabled", False)

    @classmethod
    def from_config(cls, config: dict, dry_run: bool = True) -> "ES":
        """
        Create ES object from configuration file.
        """
        host = config["elasticsearch"]["host"]
        auth = (
            config["elasticsearch"]["auth"]["username"],
            config["elasticsearch"]["auth"]["password"],
        )
        rollover = config["elasticsearch"].get("rollover", {})
        return ES(host, auth, rollover, dry_run)

    def _check_if_status(self, status: str = "yellow") -> bool:
        """
        Check if current status is or exceeds desired status.
        """
        current_status = Status[self._current_status()]
        desired_status = Status[status.upper()]
        return current_status >= desired_status

    def _current_status(self) -> str:
        return self.es.cluster.health().get("status", "red").upper()

    def _get_ilm_write_indices(self, excluder: Excluder = None) -> dict:
        """
        Scroll through the _settings for all indices, return all of those indices with is_write_index=true for a
        given alias.
        """
        log = self._log.bind(func="_get_ilm_write_indices")
        if not excluder:
            excluder = Excluder()
        ilm_write_aliases = {}
        ilm_write_indices_without_ro_alias = {}
        aliases_info = self.es.indices.get_alias(
            name="*", filter_path=["**.is_write_index"]
        )
        lifecycle_info = self.es.indices.get_settings(
            index="*", filter_path=["**.lifecycle"]
        )
        for index, aliases in aliases_info.items():
            for alias_name, settings in aliases["aliases"].items():
                if settings.get("is_write_index", False):
                    ilm_write_aliases[alias_name] = index

        for a, i in ilm_write_aliases.items():
            # TODO: remove this from the fix_ilm_method later
            if excluder(a) or excluder(i):
                continue

            try:
                if (
                    "rollover_alias"
                    not in lifecycle_info[i]["settings"]["index"]["lifecycle"]
                ):
                    ilm_write_indices_without_ro_alias[a] = i
            except KeyError:
                log.warning(
                    "key_error_on_write_alias_settings",
                    message="could not find settings for alias, index is likely missing all lifecycle information, but set as the write alias",
                    alias=a,
                    index=i,
                )

        return ilm_write_indices_without_ro_alias

    def fix_ilm_indices(
        self, excluder: Excluder = None, only_if_status: str = "yellow"
    ) -> list:
        """
        Go through ILM write aliases, add appropriate rollover_alias setting, submit a ILM retry after.
        """
        log = self._log.bind(func="fix_ilm_indices")
        if not self._check_if_status(only_if_status):
            raise ElasticsearchClusterStatus(
                self._current_status(), Status[only_if_status.upper()]
            )

        if not excluder:
            excluder = Excluder()
        write_pairs = self._get_ilm_write_indices(excluder).items()
        log.info("got_ilm_aliases", total_pairs=len(write_pairs))

        def fix(pair):
            """
            Add rollover_alias + retry ILM step.
            """
            alias, index = pair
            payload = {"settings": {"index.lifecycle.rollover_alias": alias}}
            try:
                if not self.dry_run:
                    self.es.indices.put_settings(payload, index=index)
                    self.es.xpack.ilm.retry(index=index, ignore=[400])
                log.debug("fixed_ilm_index", alias=alias, index=index, payload=payload)
            except Exception as e:
                log.exception("ilm_fix_error")

        pool = ThreadPool(min(cpu_count(), len(write_pairs)))
        tasks = []
        for pair in write_pairs:
            tasks.append(pool.spawn(fix, pair))
        pool.join()

        log.debug("submitted_ilm_settings", pairs=list(write_pairs))

    # def _get_rollover_aliases_pairs_for_old_style(self) -> dict:
    #     """
    #     Get all rollover-able aliases in alias:index pairing whose indices are open. Aliases are only rollable if
    #     they are:

    #         1. Rollable according to curator.utils.rollable_alias
    #         2. Stats can be fetched about the index
    #         3. The alias name is equal to the index name with the date math regex removed, e.g. abc == abc,
    #            custom made aliases such as request-id should fail as request-id != abc
    #     """
    #     # TODO: This needs to be a set of aliases instead, with ILM all indices will have an alias, so this will be excessive!
    #     aliases_info = self.es.cat.aliases(format="json", h="a,i")
    #     aliases = {
    #         alias["a"]: alias["i"]
    #         for alias in aliases_info
    #         if rollable_alias(self.es, alias["a"])
    #         and not self.es.indices.stats(alias["i"], ignore=400).get("error", False)
    #         and alias["a"] == self.rollover_date_math_regex_removal.sub("", alias["i"])
    #     }
    #     return aliases

    def _get_rollover_aliases_pairs_for_ilm(self) -> dict:
        """
        Use _aliases API to get any/all aliases with is_write_index=true, these should be the writable aliases.
        Shouldn't have to futz around with the old style where we're using curator utils to check if it's rollable
        and/or if the index is broken via _stats.
        """
        aliases_info = self.es.indices.get_alias(
            index="*", filter_path=["**.is_write_index"]
        ).items()
        aliases = {}
        for index, aliases_group in aliases_info:
            alias_name = next(iter(aliases_group["aliases"].keys()))
            aliases[alias_name] = index
        return aliases

    def _get_data_streams_aliases_pairs(self) -> dict:
        """
        Use data streams API to pull aliases. Builds a literal dict of stream -> stream.
        Likely incompatible with funcs requiring a alias -> index map, I don't care.
        """
        streams = {s: s for s in self._get_data_streams()}
        return streams

    def _get_rollover_aliases_pairs(self) -> dict:
        """
        Depending on ILM setting, get pairs.
        """
        if self.data_streams_enabled:
            return self._get_data_streams_aliases_pairs()
        if self.ilm_rollover_enabled:
            return self._get_rollover_aliases_pairs_for_ilm()
        raise NotImplementedError(
            "Old style rollable aliases support no longer included!"
        )

    def _get_new_indices(self) -> dict:
        """
        Find all new indices that are not currently rollover.
        """
        log = self._log.bind(func="_get_new_indices")
        current_aliases = self._get_rollover_aliases_pairs()
        new_indices = {}

        # Use ascending order, as the last object will be the most recent
        for index in self.es.cat.indices(format="json", h="i,pri,status", s="cd:asc"):

            name = index["i"]

            # Closed indices can't be rolled
            if index["status"] == "close":
                log.debug("skipped_index", reason="closed, cannot access", index=name)
                continue

            shards = int(index["pri"])

            # If it's already part of a current alias, forget it
            if name in current_aliases.values():
                for pa, pi in current_aliases.items():
                    if pi == name:
                        alias = pa
                        break
                log.debug(
                    "skipped_index",
                    reason=f"index belongs to alias {alias}",
                    index=name,
                )
                continue

            # Dot prefixed indices are generally special, think .kibana, .tasks
            if name.startswith("."):
                log.debug(
                    "skipped_index", reason="dot prefixed, likely special", index=name
                )
                continue

            # If it's date math regex like, it's probably part of an existing alias or dangling
            if self.rollover_date_math_regex.match(name):
                log.debug(
                    "skipped_index",
                    reason="matches date math regex, likely dangling or previous rolled index",
                    index=name,
                )
                continue

            if self.rollover_date_indexed_regex.match(name):
                log.debug(
                    "skipped_index",
                    reason="matches date indexed regex, likely not meant to be rollover",
                    index=name,
                )
                continue

            # Finally, we can declare it's new
            if name not in current_aliases:
                log.debug(
                    "added_index",
                    index=name,
                    shards=shards,
                    already_found=bool(new_indices.get(name, False)),
                )
                new_indices.setdefault(name, shards)

        return new_indices

    def delete_empty_indices(self, excluder: Excluder = None):
        """
        Delete empty indices prior to rolling over.
        """
        raise NotImplementedError

    def create_rollover_indices(
        self, excluder: Excluder = None, only_if_status: str = "yellow"
    ) -> list:
        """
        Create the new date math indices, reindex the original index to the
        date math index, delete the old index, then create the alias that
        points the old index name to the date math index.

        For example:
            1. 'some-index' is found, seen as not an alias
            1a. Filter out all indices that match our exclusion params
            2. '<some-index-{now/d}-000001>' index is created
            3. 'some-index' has its contents reindexed to '<some-index-{now/d}-000001>'
            4. 'some-index' is deleted and the alias 'some-index' is created pointing to the
               '<some-index-{now/d}-000001>' index atomically via the alias API
        """
        log = self._log.bind(func="create_rollover_indices")
        if not self._check_if_status(only_if_status):
            raise ElasticsearchClusterStatus(
                self._current_status(), Status[only_if_status.upper()]
            )

        # Get new indices, exclude all of those we don't want
        if not excluder:
            excluder = Excluder()
        new_indices = {
            k: v for k, v in self._get_new_indices().items() if not excluder(k)
        }
        log.debug("found_new_indices", new_indices=new_indices)

        def reindex_and_link(index, slices):
            """
            Reindex index to date math pattern, then perform atomic alias update.
            """

            # Perform reindex
            # TODO: reindex type should be configurable
            rollover_index = self.rollover_date_math_pattern.format(index=index)
            destination_index_name = rollover_index.replace(
                "{now/d}", datetime.now().strftime("%Y.%m.%d")
            ).strip("<>")
            reindex_payload = {
                "conflicts": "proceed",
                "source": {"index": index},
                "dest": {"type": "log", "index": rollover_index},
            }
            log.debug(
                "started_reindex",
                original_index=index,
                rollover_index=rollover_index,
                payload=reindex_payload,
            )
            if not self.dry_run:
                start_time = datetime.now()
                task_id = self.es.reindex(
                    body=reindex_payload,
                    slices=shards,
                    ignore=[400],
                    wait_for_completion=False,
                )["task"]
                log.debug(
                    "reindex_task_obtained",
                    original_index=index,
                    rollover_index=rollover_index,
                    payload=reindex_payload,
                    task_id=task_id,
                )

                # We'll need to check it every so often, confirm it's finished, then move on;
                # 404s indicate the task is finished and expired from .tasks
                cnt = 0
                while True:

                    # Every 30 seconds give an update on the task
                    if cnt == 6:
                        cnt = 0
                        log.debug(
                            "still_waiting_on_task",
                            running_duration=(
                                datetime.now() - start_time
                            ).total_seconds(),
                            original_index=index,
                            rollover_index=rollover_index,
                            payload=reindex_payload,
                            task_id=task_id,
                        )
                    cnt += 1

                    # Die after an hour (seriously)
                    if ((datetime.now() - start_time).total_seconds() / 60) > 60.0:
                        log.error(
                            "waited_too_long",
                            total_duration=(
                                datetime.now() - start_time
                            ).total_seconds(),
                            original_index=index,
                            rollover_index_parsed=destination_index_name,
                            message="waited too long, risking taking forever, deleting destination index and task",
                        )
                        self.es.tasks.cancel(task_id, ignore=[404])
                        self.es.indices.delete(destination_index_name)
                        break

                    # I hate doing blind exceptions, but I don't want to catch every possible
                    # error for a 5xx (either from rein or es)
                    try:
                        task_status = self.es.tasks.get(task_id, ignore=[404, 400])
                    except:
                        time.sleep(5)
                        continue

                    if (
                        task_status.get("status") in [400, 404]
                        or task_status.get("completed") == True
                    ):
                        total_time = (datetime.now() - start_time).total_seconds()
                        log.debug(
                            "reindex_task_completed",
                            task_id=task_id,
                            reindex_duration=total_time,
                        )
                        break

                    time.sleep(5)

            # Remove old index, link new alias to rollover index
            # TODO: should get the alias back from the API if possible, as the time between reindex could be enough to
            # transition into another day
            alias_payload = {
                "actions": [
                    {"remove_index": {"index": index}},
                    {"add": {"index": rollover_index, "alias": index}},
                ]
            }
            if not self.dry_run:
                try:
                    if self.es.indices.exists(index=rollover_index):
                        self.es.indices.update_aliases(
                            body=alias_payload, timeout="300s"
                        )
                    else:
                        log.error(
                            "error_rollover_index_missing_after_reindex",
                            index=index,
                            rollover_index=rollover_index,
                        )
                except Exception as e:
                    log.exception(
                        "error_updating_aliases",
                        index=index,
                        rollover_index=rollover_index,
                        payload=alias_payload,
                    )
            log.debug("created_alias", payload=alias_payload)

        # For each index, create the date math equivalent, spawn a reindex task, block until all finish
        pool = ThreadPool(min(cpu_count(), len(new_indices)))
        for index, shards in new_indices.items():
            pool.spawn(reindex_and_link, index, shards)
        pool.join()

        return list(new_indices.keys())

    def update_rollover_aliases(
        self, excluder: Excluder = None, only_if_status: str = "yellow"
    ):
        """
        For all rollable aliases on the cluster, roll them accordingly.
        """
        log = self._log.bind(func="update_rollover_aliases")
        if not self._check_if_status(only_if_status):
            raise ElasticsearchClusterStatus(
                self._current_status(), Status[only_if_status.upper()]
            )

        if not excluder:
            excluder = Excluder()
        rollable_aliases = {
            k: v
            for k, v in self._get_rollover_aliases_pairs().items()
            if not excluder(k)
        }

        # The split will only occur if you're using a unit, otherwise I would assume in raw bytes
        split = None
        for i, c in enumerate(self.rollover_target_shard_size):
            if c.isalpha():
                split = i
                break

        if split:
            amt, unit = (
                int(self.rollover_target_shard_size[:split]),
                self.rollover_target_shard_size[split::],
            )
        else:
            amt = int(self.rollover_target_shard_size)
            unit = ""

        # Roll each alias
        def roll_alias(alias, index):
            """
            Wrapper for rolling aliases.
            """
            shards = int(
                self.es.indices.get_settings(index=alias, name="*.number_of_shards")[
                    index
                ]["settings"]["index"]["number_of_shards"]
            )
            max_size = str(amt * shards) + unit
            payload = {
                "conditions": {"max_age": self.rollover_max_age, "max_size": max_size}
            }
            rv = {}
            if not self.dry_run:
                log.info(alias=alias, payload=payload)
                try:
                    rv = self.es.indices.rollover(alias=alias, body=payload)
                except Exception as e:
                    log.exception("rollover_error", exc_info=e, reason=e.info)
            log.debug("submitted_rollover_request", alias=alias, payload=payload, rv=rv)

            if not self.dry_run:
                return (alias, rv.get("rolled_over", False))

        pool = ThreadPool(min(cpu_count(), len(rollable_aliases)))
        tasks = []
        for alias, index in rollable_aliases.items():
            tasks.append(pool.spawn(roll_alias, alias, index))
        pool.join()

        if not self.dry_run:
            for task in tasks:
                alias, rolled_over = task.result()
                log.debug("was_it_rolled_over", alias=alias, rolled_over=rolled_over)

    def _get_data_streams(self) -> list:
        """
        Get all data stream names.
        """
        streams = [
            i["name"]
            for i in self.es.indices.get_data_stream(name="*", filter_path=["*.name"])[
                "data_streams"
            ]
        ]
        return streams

    def create_datastream_aliases(
        self, template: str, excluder: Excluder = None, only_if_status: str = "yellow"
    ):
        """
        For datastream related indices, create the alias according to the provided template.
        """
        log = self._log.bind(func="create_datastream_aliases")
        if not self._check_if_status(only_if_status):
            raise ElasticsearchClusterStatus(
                self._current_status(), Status[only_if_status.upper()]
            )

        if not excluder:
            excluder = Excluder()
        streams = {
            s: template.format(name=s)
            for s in self._get_data_streams()
            if not excluder(s)
        }

        # Create alias for each stream
        def create_alias(source, alias):
            """
            Wrapper for creating aliases.
            """
            alias_payload = {
                "actions": [
                    {
                        "add": {
                            "index": source,
                            "alias": alias,
                        },
                    }
                ],
            }
            if not self.dry_run:
                try:
                    self.es.indices.update_aliases(body=alias_payload, timeout="300s")
                except Exception as e:
                    log.exception(
                        "error_create_datastream_alias",
                        payload=alias_payload,
                        datastream=source,
                        alias=alias,
                    )
            log.debug("created_datastream_alias", payload=alias_payload)

        # For each datastream, create its corresponding alias
        pool = ThreadPool(min(cpu_count(), len(streams)))
        for source, alias in streams.items():
            pool.spawn(create_alias, source, alias)
        pool.join()

        return list(streams.values())


class KB:
    """
    For managing, creating and refreshing patterns with Kibana. Requires the ES information if using it for CCS.
    """

    def __init__(
        self,
        host: str,
        auth: tuple,
        is_remote: bool = False,
        es: ES = None,
        version: int = 6,
        probing: bool = False,
        prefix: str = None,
        dry_run: bool = True,
    ):
        _host = parse_url(host)
        self._log = log.new(kibana=_host.host, dry_run="ON" if dry_run else "OFF")
        self.dry_run = dry_run

        # Kibana settings
        self.host = host
        self.auth = auth
        self.version = version
        self.probing = probing
        self.prefix = prefix

        self.is_remote = is_remote
        self.cluster = es.cluster if self.is_remote else None

        # Verify saved objects API is reachable, I wish there was a better way to handle connerrors with requests
        try:
            ping_rs, ping_msg = self.ping()
            if not ping_rs:
                raise KibanaAPIError(host, ping_msg)
        except ConnectionError:
            raise KibanaAPIError(host)

    @classmethod
    def from_config(
        cls,
        config: dict,
        is_remote: bool,
        es: ES = None,
        version: int = 6,
        probing: bool = False,
        dry_run: bool = True,
    ) -> "KB":
        """
        Create KB object from config.
        """
        local_or_remote = "local_kibana" if not is_remote else "remote_kibana"
        host = config[local_or_remote]["host"]
        auth = (
            config[local_or_remote]["auth"]["username"],
            config[local_or_remote]["auth"]["password"],
        )
        probing = config[local_or_remote].get("probing", False)
        version = config[local_or_remote].get("version", 6)
        prefix = config[local_or_remote].get("prefix", None)
        return KB(host, auth, is_remote, es, version, probing, prefix, dry_run)

    def ping(self) -> Tuple[bool, str]:
        """
        Is the API accessible? Check by getting no objects back from the saved objects API. Also determine version of
        Kibana by probing if probing is on. It'd be swell if Elastic would just give us the Kibana version in the API.
        That's too much to ask for though.
        """
        log = self._log.bind(func="ping")

        def ping_5():
            log.debug("pinging_kibana", message="attempting 5.x ping")
            rv = requests.get(
                f"{self.host}/api/saved_objects/index-pattern",
                auth=self.auth,
                params={"per_page": 0},
                timeout=30,
            )
            return rv.status_code == requests.codes.ok, rv.content

        def ping_6():
            log.debug("pinging_kibana", message="attempting 6.x ping")
            rv = requests.get(
                f"{self.host}/api/saved_objects/_find",
                auth=self.auth,
                params={"type": "index-pattern", "per_page": 0},
                timeout=30,
            )
            return rv.status_code == requests.codes.ok, rv.content

        if self.probing:
            log.debug("probing_kibana")

            p5, p5_rs = ping_5()
            if p5:
                log.debug("probing_kibana", message="API looks like 5.x")
                self.version = 5
                return True, None

            p6, p6_rs = ping_6()
            if ping_6():
                log.debug("probing_kibana", message="API looks like 6.x")
                self.version = 6
                return True, None

            log.error("probing_kibana", message="no luck")
            return False, p5_rs or p6_rs

        log.debug("pinging_kibana", message=f"pinging API version {self.version}")
        if self.version == 5:
            return ping_5()
        else:
            return ping_6()

    def get_objects(
        self,
        obj: str = "index-pattern",
        per_page: int = 50000,
        page: int = 1,
        fields: str = "title,timeFieldName",
    ) -> list:
        """
        Get objects back from API. If we have more than 10000 patterns, I think we need to have a talk.
        Fields param needs to specified explicitly, as the API uses multiple definitions of fields to build
        the list of fields, vs you know, just using a comma delimited setup.
        """
        if self.version == 5:
            fields_param = "?" + "&".join([f"fields={f}" for f in fields.split(",")])
            return requests.get(
                f"{self.host}/api/saved_objects/{obj}",
                auth=self.auth,
                params={"per_page": per_page, "page": page},
                timeout=30,
            ).json()

        fields_param = "?" + "&".join([f"fields={f}" for f in fields.split(",")])
        return requests.get(
            f"{self.host}/api/saved_objects/_find{fields_param}",
            auth=self.auth,
            params={"type": obj, "per_page": per_page, "page": page},
            timeout=30,
        ).json()

    def create_patterns(
        self,
        incoming_names: list,
        prepopulated_names: list = None,
        current_patterns: list = None,
        template: str = None,
        use_template: bool = False,
        excluder: Excluder = None,
    ) -> dict:
        """
        Create index pattern(s), will not recreate patterns if a pattern already exists.
        """
        log = self._log.bind(func="create_patterns")
        if not excluder:
            excluder = Excluder()

        # Each pattern requires its own payload, if using a prefix, just add -*, fuck it
        def remote_name(s):
            if self.is_remote and self.prefix:
                return f"{self.prefix}:{s}-*"
            if self.is_remote and use_template:
                return template.format(cluster=self.cluster, index=s)
            return f"{self.cluster}:{s}" if self.is_remote else s

        # Names should only be defined if we're creating patterns via this action only
        # if we're passing names from the recreate action, then we shouldn't need this a second time
        #
        # I HATE SPAGHETTI, I WANT TO REWRITE THIS INTO SQUIGGGLER AND NOT 2.0!
        if not prepopulated_names:
            current_patterns = [
                p["attributes"]["title"].strip("*-")
                for p in self.get_objects()["saved_objects"]
            ]
            names = {
                remote_name(name): None
                for name in incoming_names
                if not excluder(name) and remote_name(name) not in current_patterns
            }
        else:
            names = {n: None for n in prepopulated_names}
        if names:
            log.debug(
                "found_unmade_patterns",
                incoming_names=list(names.keys()),
                current_patterns=current_patterns,
            )

        pool = ThreadPool(min(cpu_count(), len(names)))
        for name in names:
            title = f"{name}-*"
            if use_template:
                title = name
            payload = {"attributes": {"title": title, "timeFieldName": "@timestamp"}}
            if self.version == 5:
                payload["attributes"]["notExpandable"] = True
            if not self.dry_run:
                names[name] = pool.spawn(
                    requests.post,
                    f"{self.host}/api/saved_objects/index-pattern/{title}",
                    json=payload,
                    auth=self.auth,
                    headers={"kbn-xsrf": "anything"},
                    timeout=30,
                )
            log.debug("created_pattern", name=name, payload=payload)
        pool.join()

        for name in names:
            if not self.dry_run:
                # When creating the index pattern with the name at the end we make the id/uuid the
                # name of the index pattern, so effectively it should be abc-* => abc-*
                names[name] = names[name].result().json().get("id")

        return names

    def recreate_missing_patterns(
        self,
        es: ES,
        template: str,
        use_template: bool = False,
        excluder: Excluder = None,
    ) -> dict:
        """
        Recreate missing rollover related patterns from cluster.
        """
        log = self._log.bind(func="recreate_missing_patterns")
        if not excluder:
            excluder = Excluder()

        # Each pattern requires its own payload, if using a prefix, just add -*, fuck it
        def remote_name(s):
            if self.is_remote and self.prefix:
                return f"{self.prefix}:{s}-*"
            if self.is_remote and use_template:
                return template.format(cluster=self.cluster, index=s)
            return f"{self.cluster}:{s}" if self.is_remote else s

        rollover_aliases = [
            remote_name(a) for a in es._get_rollover_aliases_pairs().keys()
        ]
        if not use_template:
            current_patterns = [
                p["attributes"]["title"].strip("*-")
                for p in self.get_objects()["saved_objects"]
            ]
        else:
            current_patterns = [
                p["attributes"]["title"] for p in self.get_objects()["saved_objects"]
            ]
        missing_patterns = [p for p in rollover_aliases if p not in current_patterns]

        # remove the cluster name if we added it
        cleaned_missing_patterns = missing_patterns.copy()
        if self.is_remote:
            cleaned_missing_patterns = [p.split(":", 1)[-1] for p in missing_patterns]

        log.debug("found_missing_patterns", missing_patterns=missing_patterns)
        return self.create_patterns(
            cleaned_missing_patterns,
            prepopulated_names=missing_patterns,
            current_patterns=current_patterns,
            template=template,
            use_template=use_template,
            excluder=excluder,
        )

    def refresh_patterns(self, excluder: Excluder = None):
        """
        Refresh all index patterns.
        """
        log = self._log.bind(func="refresh_patterns")
        if not excluder:
            excluder = Excluder()

        # TODO: could likely be pool'd
        patterns = [
            p
            for p in self.get_objects()["saved_objects"]
            if not excluder(p["attributes"]["title"])
        ]
        patterns = [(p["id"], {"attributes": p["attributes"]}) for p in patterns]
        for pattern_id, payload in patterns:
            if not self.dry_run:
                requests.put(
                    f"{self.host}/api/saved_objects/index-pattern/{pattern_id}",
                    json=payload,
                    auth=self.auth,
                    headers={"kbn-xsrf": "anything"},
                    timeout=30,
                )
            log.debug(
                "refreshed_pattern",
                pattern_id=pattern_id,
                title=payload["attributes"]["title"],
                payload=payload,
            )


def parse_config(config: str) -> dict:
    """
    Parse configuration, supplying defaults where necessary.
    """
    data = YAML(typ="safe").load(os.path.expandvars(Path(config).read_bytes()))
    schema = {
        "elasticsearch": {
            "type": "dict",
            "schema": {
                "host": {"type": "string", "required": True},
                "auth": {
                    "type": "dict",
                    "required": True,
                    "schema": {
                        "username": {"type": "string", "required": True},
                        "password": {"type": "string", "required": True},
                    },
                },
                "rollover": {
                    "type": "dict",
                    "schema": {
                        "max_age": {"type": "string", "default": "30d"},
                        "target_shard_size": {
                            "type": ["string", "integer"],
                            "default": "10gb",
                            "coerce": str,
                        },
                        "date_math_pattern": {
                            "type": "string",
                            "default": "<{index}-{{now/d}}-000001>",
                        },
                        "date_math_regex": {
                            "type": "string",
                            "default": ".*\-\d{4}\.\d{2}\.\d{2}\-\d{6}$",
                        },
                        "date_math_regex_removal": {
                            "type": "string",
                            "default": "\-\d{4}\.\d{2}\.\d{2}\-\d{6}$",
                        },
                        "ilm_rollover_enabled": {"type": "boolean", "default": False},
                        "data_streams_enabled": {"type": "boolean", "default": False},
                    },
                },
            },
        },
        "local_kibana": {
            "type": "dict",
            "required": False,
            "schema": {
                "host": {
                    "type": "string",
                    "dependencies": ["^local_kibana", "^elasticsearch"],
                },
                "probing": {"type": "boolean", "required": False, "default": False},
                "version": {
                    "type": "integer",
                    "required": False,
                    "default": 6,
                    "min": 5,
                    "max": 6,
                },
                "auth": {
                    "type": "dict",
                    "dependencies": ["^local_kibana", "^elasticsearch"],
                    "schema": {
                        "username": {"type": "string", "required": True},
                        "password": {"type": "string", "required": True},
                    },
                },
            },
        },
        "remote_kibana": {
            "type": "dict",
            "required": False,
            "schema": {
                "prefix": {"type": "string", "required": False},
                "host": {
                    "type": "string",
                    "dependencies": ["^remote_kibana", "^elasticsearch"],
                },
                "probing": {"type": "boolean", "required": False, "default": False},
                "version": {
                    "type": "integer",
                    "required": False,
                    "default": 6,
                    "min": 5,
                    "max": 6,
                },
                "auth": {
                    "type": "dict",
                    "dependencies": ["^remote_kibana", "^elasticsearch"],
                    "schema": {
                        "username": {"type": "string", "required": True},
                        "password": {"type": "string", "required": True},
                    },
                },
            },
        },
        "actions": {
            "type": "dict",
            "required": False,
            "schema": {
                "delete_empty_indices": {
                    "type": "dict",
                    "required": False,
                    "schema": {
                        "only_if_status": {
                            "type": "string",
                            "default": "yellow",
                            "allowed": ["red", "yellow", "green"],
                        },
                        "enabled": {"type": "boolean", "default": False},
                        "exclude_method": {
                            "type": "string",
                            "allowed": ["prefix", "suffix", "regex"],
                        },
                        "exclude_patterns": {
                            "type": "list",
                            "empty": False,
                            "dependencies": ["exclude_method"],
                        },
                    },
                },
                "create_rollover_indices": {
                    "type": "dict",
                    "required": False,
                    "schema": {
                        "only_if_status": {
                            "type": "string",
                            "default": "green",
                            "allowed": ["red", "yellow", "green"],
                        },
                        "enabled": {"type": "boolean", "default": False},
                        "exclude_method": {
                            "type": "string",
                            "allowed": ["prefix", "suffix", "regex"],
                        },
                        "exclude_patterns": {
                            "type": "list",
                            "empty": False,
                            "dependencies": ["exclude_method"],
                        },
                    },
                },
                "update_rollover_aliases": {
                    "type": "dict",
                    "required": False,
                    "schema": {
                        "only_if_status": {
                            "type": "string",
                            "default": "yellow",
                            "allowed": ["red", "yellow", "green"],
                        },
                        "enabled": {"type": "boolean", "default": False},
                        "exclude_method": {
                            "type": "string",
                            "allowed": ["prefix", "suffix", "regex"],
                        },
                        "exclude_patterns": {
                            "type": "list",
                            "empty": False,
                            "dependencies": ["exclude_method"],
                        },
                    },
                },
                "fix_ilm_indices": {
                    "type": "dict",
                    "required": False,
                    "schema": {
                        "only_if_status": {
                            "type": "string",
                            "default": "yellow",
                            "allowed": ["red", "yellow", "green"],
                        },
                        "enabled": {"type": "boolean", "default": False},
                        "exclude_method": {
                            "type": "string",
                            "allowed": ["prefix", "suffix", "regex"],
                        },
                        "exclude_patterns": {
                            "type": "list",
                            "empty": False,
                            "dependencies": ["exclude_method"],
                        },
                    },
                },
                "create_local_kibana_patterns": {
                    "type": "dict",
                    "required": False,
                    "schema": {
                        "enabled": {"type": "boolean", "default": False},
                        "exclude_method": {
                            "type": "string",
                            "allowed": ["prefix", "suffix", "regex"],
                        },
                        "exclude_patterns": {
                            "type": "list",
                            "empty": False,
                            "dependencies": ["exclude_method"],
                        },
                    },
                },
                "create_datastream_aliases": {
                    "type": "dict",
                    "required": False,
                    "schema": {
                        "enabled": {"type": "boolean", "default": False},
                        "only_if_status": {
                            "type": "string",
                            "default": "yellow",
                            "allowed": ["red", "yellow", "green"],
                        },
                        "exclude_method": {
                            "type": "string",
                            "allowed": ["prefix", "suffix", "regex"],
                        },
                        "exclude_patterns": {
                            "type": "list",
                            "empty": False,
                            "dependencies": ["exclude_method"],
                        },
                        "template": {
                            "type": "string",
                            "required": True,
                            "empty": False,
                        },
                    },
                },
                "create_remote_kibana_patterns": {
                    "type": "dict",
                    "required": False,
                    "schema": {
                        "enabled": {"type": "boolean", "default": False},
                        "exclude_method": {
                            "type": "string",
                            "allowed": ["prefix", "suffix", "regex"],
                        },
                        "exclude_patterns": {
                            "type": "list",
                            "empty": False,
                            "dependencies": ["exclude_method"],
                        },
                        "template": {
                            "type": "string",
                            "required": False,
                            "default": "{cluster}:{index}-*",
                            "empty": False,
                        },
                    },
                },
                "recreate_remote_kibana_patterns": {
                    "type": "dict",
                    "required": False,
                    "schema": {
                        "enabled": {"type": "boolean", "default": False},
                        "exclude_method": {
                            "type": "string",
                            "allowed": ["prefix", "suffix", "regex"],
                        },
                        "exclude_patterns": {
                            "type": "list",
                            "empty": False,
                            "dependencies": ["exclude_method"],
                        },
                        "template": {
                            "type": "string",
                            "required": False,
                            "default": "{cluster}:{index}-*",
                            "empty": False,
                        },
                    },
                },
                "recreate_local_kibana_patterns": {
                    "type": "dict",
                    "required": False,
                    "schema": {
                        "enabled": {"type": "boolean", "default": False},
                        "exclude_method": {
                            "type": "string",
                            "allowed": ["prefix", "suffix", "regex"],
                        },
                        "exclude_patterns": {
                            "type": "list",
                            "empty": False,
                            "dependencies": ["exclude_method"],
                        },
                    },
                },
                "refresh_local_kibana_patterns": {
                    "type": "dict",
                    "required": False,
                    "schema": {
                        "enabled": {"type": "boolean", "default": False},
                        "exclude_method": {
                            "type": "string",
                            "allowed": ["prefix", "suffix", "regex"],
                        },
                        "exclude_patterns": {
                            "type": "list",
                            "empty": False,
                            "dependencies": ["exclude_method"],
                        },
                    },
                },
                "refresh_remote_kibana_patterns": {
                    "type": "dict",
                    "required": False,
                    "schema": {
                        "enabled": {"type": "boolean", "default": False},
                        "exclude_method": {
                            "type": "string",
                            "allowed": ["prefix", "suffix", "regex"],
                        },
                        "exclude_patterns": {
                            "type": "list",
                            "empty": False,
                            "dependencies": ["exclude_method"],
                        },
                    },
                },
            },
        },
        "outputs": {
            "type": "dict",
            "schema": {
                "slack": {
                    "type": "dict",
                    "schema": {
                        "webhook": {"type": "string", "empty": False, "required": True},
                        "message": {
                            "type": "string",
                            "empty": False,
                            "default": "{emoji} New pattern(s) made: {patterns}",
                        },
                        "emojis": {
                            "type": "string",
                            "default": "🚀,😄,💪,🙏,🙌,🙈,😂,😎,😱,😈",
                            "empty": True,
                        },
                        "notify_on_local": {"type": "boolean", "default": False},
                        "notify_on_remote": {"type": "boolean", "default": True},
                    },
                },
                "statsd": {
                    "type": "dict",
                    "schema": {
                        "host": {"type": "string", "required": True},
                        "prefix": {
                            "type": "string",
                            "empty": False,
                            "default": "squiggler",
                            "coerce": lambda s: s.strip("."),
                        },
                        "name": {"type": "string", "required": True},
                    },
                },
            },
        },
    }

    v = Validator(schema)
    if not v.validate(data):
        raise SystemExit(f"Errors validating config: {v.errors}")

    config = v.document.copy()

    # Remove disabled actions
    to_remove = []
    for action, action_config in config["actions"].items():
        if not action_config.get("enabled", False):
            to_remove.append(action)

    for key in to_remove:
        log.debug("removed_action", action=key)
        del config["actions"][key]

    # TODO: check relationships between actions and configuration items

    return config


def squigglerate(config: str, dry_run: bool = True):
    """
    Parse config, do actions, make money, that kind of stuff.
    """

    # Setup
    config = parse_config(config)
    actions = config["actions"]
    timings = {}

    @contextmanager
    def better_managed_action(item: str, loc: dict = actions):
        """
        Handles timing + exception handling for given item in dict. But also gives
        excluder, timing, dict and all the jazz I'd have to make by hand.
        """
        start = time.time()
        try:
            if item in loc:
                excluder = Excluder.from_config(actions[item], item)
                dump = namedtuple("dump", ["name", "action", "excluder", "only_if"])
                # Use get on only_if_status as some actions do not touch ES, so only_if might be none which is fine
                yield dump(
                    item, actions[item], excluder, actions[item].get("only_if_status")
                )
            else:
                # Works by passing None, which fails the truthy test
                yield
        except ElasticsearchClusterStatus as e:
            log.error(
                "cluster_health_error",
                current_status=e.current_status,
                desired_status=e.desired_status,
            )
        except Exception as e:
            log.error(f"{item}_error", exc_info=True)
        finally:
            if item in loc:
                end = time.time()
                log.debug(f"{item}_finished", duration=end - start)
                timings[item] = end - start

    if "elasticsearch" in config:
        es = ES.from_config(config, dry_run)

    if "local_kibana" in config:
        local_kibana = KB.from_config(config, is_remote=False, dry_run=dry_run)

    if "remote_kibana" in config:
        remote_kibana = KB.from_config(config, is_remote=True, es=es, dry_run=dry_run)

    if "slack" in config.get("outputs", {}):
        slack = Slack.from_config(config["outputs"]["slack"], dry_run)
    else:
        slack = Slack(
            "", notify_on_local=False, notify_on_remote=False, dry_run=dry_run
        )

    if "statsd" in config.get("outputs", {}):
        statsd = Statsd.from_config(config["outputs"]["statsd"], dry_run)
    else:
        statsd = Statsd("", "", "", dry_run)

    indices = []

    # Rollover process
    with better_managed_action("create_rollover_indices") as action:
        if action:
            indices = es.create_rollover_indices(action.excluder, action.only_if)

    with better_managed_action("update_rollover_aliases") as action:
        if action:
            es.update_rollover_aliases(action.excluder, action.only_if)

    with better_managed_action("fix_ilm_indices") as action:
        if action:
            es.fix_ilm_indices(action.excluder, action.only_if)

    # Create patterns
    with better_managed_action("create_local_kibana_patterns") as action:
        if action:
            if len(indices) > 0:
                patterns = local_kibana.create_patterns(
                    incoming_names=indices, excluder=action.excluder
                )
                if all([slack, patterns, slack.notify_on_local]):
                    slack.notify(patterns, local_kibana.host)

    with better_managed_action("create_remote_kibana_patterns") as action:
        if action:
            if len(indices) > 0:
                patterns = remote_kibana.create_patterns(
                    incoming_names=indices,
                    excluder=action.excluder,
                    template=action.action["template"],
                    use_template="template" in action.action,
                )
                if all([slack, patterns, slack.notify_on_remote]):
                    slack.notify(patterns, remote_kibana.host)

    with better_managed_action("create_datastream_aliases") as action:
        if action:
            es.create_datastream_aliases(
                action.action["template"], action.excluder, action.only_if
            )

    with better_managed_action("recreate_local_kibana_patterns") as action:
        if action:
            patterns = local_kibana.recreate_missing_patterns(es, action.excluder)
            if all([slack, patterns, slack.notify_on_local]):
                slack.notify(patterns, local_kibana.host)

    with better_managed_action("recreate_remote_kibana_patterns") as action:
        if action:
            patterns = remote_kibana.recreate_missing_patterns(
                es=es,
                template=action.action["template"],
                use_template="template" in action.action,
                excluder=action.excluder,
            )
            if all([slack, patterns, slack.notify_on_remote]):
                slack.notify(patterns, remote_kibana.host)

    # Refresh patterns
    with better_managed_action("refresh_local_kibana_patterns") as action:
        if action:
            local_kibana.refresh_patterns(action.excluder)

    with better_managed_action("refresh_remote_kibana_patterns") as action:
        if action:
            remote_kibana.refresh_patterns(action.excluder)

    log.info("finished", total_duration=sum(timings.values()), action_durations=timings)
    if "statsd" in config.get("outputs", {}):
        statsd.notify(
            [("status", 1), *timings.items(), ("total_duration", sum(timings.values()))]
        )


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("config")
    parser.add_argument(
        "--for-real", default=True, action="store_false", help="turn off dry run"
    )
    parser.add_argument(
        "--verbose", default=False, action="store_true", help="verbose logging"
    )
    args = parser.parse_args()
    if args.verbose:
        log.setLevel(logging.DEBUG)
        log.debug("verbose_mode_set")
    else:
        log.setLevel(logging.INFO)

    try:
        squigglerate(args.config, args.for_real)
    except Exception as e:
        log.exception()


if __name__ == "__main__":
    run()
