import asyncio
import copy
import os
from collections import UserDict
from copy import deepcopy
from functools import partial

from biothings.hub import INDEXER_CATEGORY, INDEXMANAGER_CATEGORY
from biothings.hub.databuild.backend import (create_backend,
                                             merge_src_build_metadata)
from biothings.utils.common import (get_class_from_classpath,
                                    get_random_string, iter_n, traverse)
from biothings.utils.es import ESIndexer
from biothings.utils.hub_db import get_src_build
from biothings.utils.loggers import get_logger
from biothings.utils.manager import BaseManager
from biothings.utils.mongo import id_feeder
from elasticsearch import AsyncElasticsearch
from pymongo.mongo_client import MongoClient

from .indexer_payload import *
from .indexer_registrar import *
from .indexer_schedule import Schedule
from .indexer_task import dispatch

# Summary
# -------
# IndexManager: a hub feature, providing top level commands and config environments(env).
# Indexer/ColdHotIndexer: the "index" command, handles jobs, db state and errors.
# .indexer_task.IndexingTask: index a set of ids, running independent of the hub.


class IndexerException(Exception):
    ...

class ProcessInfo():

    def __init__(self, indexer, concurrency):
        self.indexer = indexer
        self.concurrency = concurrency

    def get_predicates(self):
        def limit_indexer_concurrency(job_manager):
            def by_indexer_environment(job):
                return all((
                    job["category"] == INDEXER_CATEGORY,
                    job["source"] == self.indexer.env_name
                ))
            return len(list(filter(
                by_indexer_environment,
                job_manager.jobs.values()
            ))) < self.concurrency
        return [limit_indexer_concurrency]

    def get_pinfo(self, step="", description=""):
        """
        Return dict containing information about the current process
        (used to report in the hub)
        """
        pinfo = {
            "__predicates__": self.get_predicates(),
            "category": INDEXER_CATEGORY,
            "source": self.indexer.env_name,
            "description": description,
            "step": step
        }
        return pinfo


class _BuildDoc(UserDict):
    """ Represent A Build Under "src_build" Collection.

    Example:
    {
        "_id":"mynews_202105261855_5ffxvchx",
        "target_backend": "mongo",
        "target_name": "mynews_202105261855_5ffxvchx",
        "backend_url": "mynews_202105261855_5ffxvchx",
        "build_config": {
            "_id": "mynews",
            "name": "mynews",
            "doc_type": "news",
            ...
            "cold_collection": "mynews_202012280220_vsdevjdk"
        },
        "mapping": {
            "author": {"type": "text" },
            "title": {"type": "text" },
            "description": {"type": "text" },
            ...
        },
        "_meta": {
            "biothing_type": "news",
            "build_version": "202105261855",
            "build_date": "2021-05-26T18:55:00.054622+00:00",
            ...
        },
        ...
    }
    """
    @property
    def build_config(self):
        return self.get("build_config", {})

    def enrich_mappings(self, mappings):
        mappings["__hub_doc_type"] = self.build_config.get("doc_type")
        mappings["properties"].update(self.get("mapping", {}))
        mappings["_meta"] = self.get("_meta", {})

    def enrich_settings(self, settings):
        settings["number_of_shards"] = self.build_config.get("num_shards", 1)
        settings["number_of_replicas"] = self.build_config.get("num_replicas", 0)


def _db(backend_url):
    """ Standardize mongo connection string. """

    if isinstance(backend_url, str):
        # Case 1: already a mongo connection URI
        # https://docs.mongodb.com/manual/reference/connection-string/
        if backend_url.startswith("mongodb://"):
            return dict(host=backend_url)

        # Case 2: Sebastian's hub style backend URI
        # #biothings.hub.databuild.backend.create_backend
        from biothings.hub.databuild import backend
        db = backend.mongo.get_target_db()
        if backend_url in db.list_collection_names():
            return dict(host="mongodb://{}:{}/{}".format(
                *db.client.address, db.name))

    # Case 3: Use connection default
    # mongodb://localhost:27017
    elif backend_url is None:
        return dict()

    raise ValueError(backend_url)


class Indexer():
    """
    MongoDB -> Elasticsearch Indexer.
    """

    def __init__(self, build_doc, indexer_env, col_name, index_name):

        # build_doc primarily describes the source collection.
        # indexer_env primarily describes the destination index.

        # ----------source----------

        assert build_doc.get("target_backend") in ('mongo', None)
        self.mongo_client_args = _db(build_doc.get("backend_url"))
        self.mongo_collection_name = col_name

        # -----------dest-----------

        self.es_client_args = indexer_env.get("args", {})
        self.es_index_name = index_name or col_name
        self.es_index_settings = IndexSettings(deepcopy(DEFAULT_INDEX_SETTINGS))
        self.es_index_mappings = IndexMappings(deepcopy(DEFAULT_INDEX_MAPPINGS))

        _build_doc = _BuildDoc(build_doc)
        _build_doc.enrich_settings(self.es_index_settings)
        _build_doc.enrich_mappings(self.es_index_mappings)

        # ----------logging----------

        self.env_name = indexer_env.get("name")
        self.conf_name = _build_doc.build_config.get("name")
        self.logger, self.logfile = get_logger('index_%s' % self.es_index_name)

        self.pinfo = ProcessInfo(self, indexer_env.get("concurrency", 3))

    # --------------
    #  Entry Point
    # --------------

    @asyncio.coroutine
    def index(self,
              job_manager,
              steps=("pre", "index", "post"),
              batch_size=10000,
              ids=None,
              mode="index"):
        """
        Build an Elasticsearch index (self.es_index_name)
        with data from MongoDB collection (self.mongo_collection_name).

        "ids" can be passed to selectively index documents.

        "mode" can have the following values:
            - 'purge': will delete an index if it exists.
            - 'resume': will use an existing index and add missing documents.
            - 'merge': will merge data to an existing index.
            - 'index'/None (default): will create a new index.
        """

        if isinstance(steps, str):
            steps = [steps]

        assert job_manager
        assert 50 <= batch_size <= 10000, '"batch_size" out-of-range'
        assert isinstance(steps, (list, tuple)), 'bad argument "steps"'
        assert isinstance(mode, str), 'bad argument "mode"'

        # the batch size here controls only the task partitioning
        # it does not affect how the elasticsearch python client
        # makes batch requests. a number larger than 10000 may exceed
        # es result window size and doc_feeder maximum fetch size.
        # a number smaller than 50 is too small that the documents
        # can be sent to elasticsearch within one request, making it
        # inefficient, amplifying the scheduling overhead.

        cnt = 0

        if "pre" in steps:
            self.logger.info("Running pre-index process for index '%s'", self.es_index_name)
            status = PreIndexJSR(self, get_src_build())
            status.started()
            try:
                yield from self.pre_index(mode)
            except Exception as exc:
                self.logger.error(str(exc))
                status.failed(str(exc))
                raise
            else:
                status.succeed()

        if "index" in steps:
            self.logger.info("Running indexing process for index '%s'", self.es_index_name)
            status = MainIndexJSR(self, get_src_build())
            status.started()
            try:
                # the indexing stage does its own scheduling,
                # creating multiple batched jobs for indexing.
                cnt = yield from self.do_index(job_manager, batch_size, ids, mode)
            except Exception as exc:
                self.logger.error(str(exc))
                status.failed(str(exc))
                raise
            else:
                # TODO depending on the mode, this number can be misleading..
                status.succeed(index={self.es_index_name: {"count": cnt}})

        if "post" in steps:
            self.logger.info("Running post-index process for index '%s'", self.es_index_name)
            status = PostIndexJSR(self, get_src_build())
            status.started()
            try:
                res = yield from self.post_index()

            except Exception as exc:
                self.logger.exception(
                    "Post-index process failed for index '%s':",
                    self.es_index_name, extra={"notify": True})
                status.failed(str(exc))
                raise

            else:
                self.logger.info(
                    "Post-index process done for index '%s': %s",
                    self.es_index_name, res)
                status.succeed()

        return {self.es_index_name: cnt}

    # ---------
    #   Steps
    # ---------

    @asyncio.coroutine
    def pre_index(self, mode):

        client = AsyncElasticsearch(**self.es_client_args)
        try:
            if mode in ("index", None):  # index must not exist
                if (yield from client.indices.exists(self.es_index_name)):
                    msg = ("Index '%s' already exists, (use mode='purge' to "
                           "auto-delete it or mode='resume' to add more documents)")
                    raise IndexerException(msg % self.es_index_name)

            elif mode in ("resume", "merge"):  # index must exist
                if not (yield from client.indices.exists(self.es_index_name)):
                    raise IndexerException("'%s' does not exist." % self.es_index_name)
                self.logger.info("Found the existing index.")
                return  # skip index creation at the end of this method

            elif mode == "purge":  # index may exist
                response = yield from client.indices.delete(self.es_index_name, ignore_unavailable=True)
                self.logger.info(("Deleted the existing index.", response))

            else:
                raise ValueError("Invalid mode: %s" % mode)

            self.logger.info("Creating index %s.", self.es_index_name)
            return (yield from client.indices.create(self.es_index_name, body={
                "settings": (yield from self.es_index_settings.finalize(client)),
                "mappings": (yield from self.es_index_mappings.finalize(client))
            }))
        finally:
            yield from client.close()

    @asyncio.coroutine
    def do_index(self, job_manager, batch_size, ids, mode):

        client = MongoClient(**self.mongo_client_args)
        database = client.get_default_database()
        collection = database[self.mongo_collection_name]

        if ids:
            self.logger.info(
                (
                    "Indexing from '%s' with specific list of _ids, "
                    "create indexer job with batch_size=%d."
                ),
                self.mongo_collection_name, batch_size
            )
            # use user provided ids in batch
            id_provider = iter_n(ids, batch_size)
        else:
            self.logger.info(
                (
                    "Fetch _ids from '%s', and create "
                    "indexer job with batch_size=%d."
                ),
                self.mongo_collection_name, batch_size
            )
            # use ids from the target mongodb collection in batch
            id_provider = id_feeder(collection, batch_size, logger=self.logger)

        jobs = []  # asyncio.Future(s)
        error = None  # the first Exception

        total = len(ids) if ids else collection.count()
        schedule = Schedule(total, batch_size)

        def batch_finished(future):
            nonlocal error
            try:
                schedule.finished += future.result()
            except Exception as exc:
                self.logger.warning(exc)
                error = exc

        for batch_num, ids in zip(schedule, id_provider):
            yield from asyncio.sleep(0.0)

            # when one batch failed, and job scheduling has not completed,
            # stop scheduling and cancel all on-going jobs, to fail quickly.

            if error:
                for job in jobs:
                    if not job.done():
                        job.cancel()
                raise error

            self.logger.info(schedule)

            pinfo = self.pinfo.get_pinfo(
                schedule.suffix(self.mongo_collection_name))

            job = yield from job_manager.defer_to_process(
                pinfo, dispatch,
                self.mongo_client_args,
                self.mongo_collection_name,
                self.es_client_args,
                self.es_index_name,
                ids, mode, batch_num
            )
            job.add_done_callback(batch_finished)
            jobs.append(job)

        self.logger.info("Scheduled ALL %d indexing job(s).", len(jobs))
        yield from asyncio.gather(*jobs)

        schedule.completed(ignore_mismatch=(mode == 'resume'))
        self.logger.info(
            "Successfully created Index '%s' using the collection %s",
            self.es_index_name, self.mongo_collection_name,
            extra={"notify": True}
        )
        return total

    @ asyncio.coroutine
    def post_index(self):
        """
        Override in sub-class to add a post-index process.
        This method will run in a thread (using job_manager.defer_to_thread())
        """


# TODO Mapping merge should be handled in indexer
# meta merging merge_src_build_metadata not used yet..
# TODO # correct count in hot cold indexer

class ColdHotIndexer():
    """
    This indexer works with 2 mongo collections to create a single index.
    - one premerge collection contains "cold" data, which never changes (not updated)
    - another collection contains "hot" data, regularly updated
    Index is created fetching the premerge documents. Then, documents from the hot collection
    are merged by fetching docs from the index, updating them, and putting them back in the index.
    """

    def __init__(self, build_doc, indexer_env, target_name, index_name):
        cold_target = build_doc["build_config"]["cold_collection"]
        cold_build_doc = get_src_build().find_one({'_id': cold_target})
        self.index_name = index_name or target_name

        self.cold = Indexer(cold_build_doc, indexer_env, cold_target, self.index_name)
        self.hot = Indexer(build_doc, indexer_env, target_name, self.index_name)

    @ asyncio.coroutine
    def index(self,
              job_manager,
              steps=["index", "post"],
              batch_size=10000,
              ids=None,
              mode="index"):
        """
        Same as Indexer.index method but works with a cold/hot collections strategy: first index the cold collection then
        complete the index with hot collection (adding docs or merging them in existing docs within the index)
        """
        assert job_manager
        if isinstance(steps, str):
            steps = [steps]

        cnt = 0
        if "index" in steps:
            # ---------------- Sebastian's Note ---------------
            # selectively index cold then hot collections, using default index method
            # but specifically 'index' step to prevent any post-process before end of
            # index creation
            # Note: copy backend values as there are some references values between cold/hot and build_doc
            cold_task = self.cold.index(job_manager, ("pre", "index"), batch_size, ids, mode)
            cnt = yield from cold_task
            hot_task = self.hot.index(job_manager, "index", batch_size, ids, "merge")
            cnt = yield from hot_task
        if "post" in steps:
            # use super index but this time only on hot collection (this is the entry point, cold collection
            # remains hidden from outside)
            self.hot.post_index()

        return {self.index_name: cnt}


class IndexManager(BaseManager):

    # An index config is considered a "source" for the manager
    # Each call returns a different instance from a factory call

    DEFAULT_INDEXER = Indexer

    def __init__(self, *args, **kwargs):
        """
        An example of config dict for this module.
        {
            "indexer_select": {
                None: "hub.dataindex.indexer.DrugIndexer", # default
                "build_config.cold_collection" : "mv.ColdHotVariantIndexer",
            },
            "env": {
                "prod": {
                    "host": "localhost:9200",
                    "indexer": {
                        "args": {
                            "timeout": 300,
                            "retry_on_timeout": True,
                            "max_retries": 10,
                        },
                        "concurrency": 3
                    },
                    "index": [
                        # for information only, only used in index_info
                        {"index": "mydrugs_current", "doc_type": "drug"},
                        {"index": "mygene_current", "doc_type": "gene"}
                    ],
                },
                "dev": { ... }
            }
        }
        """
        super().__init__(*args, **kwargs)
        self._srcbuild = get_src_build()
        self._config = {}

        self.logger, self.logfile = get_logger('indexmanager')

    # Object Lifecycle Calls
    # --------------------------
    # manager = IndexManager(job_manager)
    # manager.clean_stale_status() # in __init__
    # manager.configure(config)

    def clean_stale_status(self):
        IndexJobStatusRegistrar.prune(get_src_build())

    def configure(self, conf):
        if not isinstance(conf, dict):
            raise TypeError(type(conf))

        # keep an original config copy
        self._config = copy.deepcopy(conf)

        # register each indexing environment
        for name, env in conf["env"].items():
            self.register[name] = env.get("indexer", {})
            self.register[name].setdefault("args", {})
            self.register[name]["args"].setdefault("hosts", env.get("host"))
            self.register[name]["name"] = name
        self.logger.info(self.register)

    # Job Manager Hooks
    # ----------------------

    def get_predicates(self):
        def no_other_indexmanager_step_running(job_manager):
            """IndexManager deals with snapshot, publishing,
            none of them should run more than one at a time"""
            return len([
                j for j in job_manager.jobs.values()
                if j["category"] == INDEXMANAGER_CATEGORY
            ]) == 0

        return [no_other_indexmanager_step_running]

    def get_pinfo(self):
        """
        Return dict containing information about the current process
        (used to report in the hub)
        """
        pinfo = {
            "category": INDEXMANAGER_CATEGORY,
            "source": "",
            "step": "",
            "description": ""
        }
        preds = self.get_predicates()
        if preds:
            pinfo["__predicates__"] = preds
        return pinfo

    # Hub Features
    # --------------

    def _select_indexer(self, target_name=None):
        """ Find the indexer class required to index target_name. """

        rules = self._config.get("indexer_select")
        if not rules or not target_name:
            self.logger.debug(self.DEFAULT_INDEXER)
            return self.DEFAULT_INDEXER

        # the presence of a path in the build doc
        # can determine the indexer class to use.

        path = None
        doc = self._srcbuild.find_one({"_id": target_name}) or {}
        for path_in_doc, _ in traverse(doc, True):
            if path_in_doc in rules:
                if not path:
                    path = path_in_doc
                else:
                    _ERR = "Multiple indexers matched."
                    raise RuntimeError(_ERR)

        kls = get_class_from_classpath(rules[path])
        self.logger.debug(kls)
        return kls

    def index(self,
              indexer_env,  # elasticsearch env
              target_name,  # source mongodb collection
              index_name=None,  # elasticsearch index name
              ids=None,  # document ids
              **kwargs):
        """
        Trigger an index creation to index the collection target_name and create an
        index named index_name (or target_name if None). Optional list of IDs can be
        passed to index specific documents.
        """

        indexer_env_ = dict(self[indexer_env])  # describes destination
        build_doc = self._srcbuild.find_one({'_id': target_name})  # describes source

        if not build_doc:
            raise ValueError("Cannot find build %s." % target_name)
        if not build_doc.get("build_config"):
            raise ValueError("Cannot find build config for '%s'." % target_name)

        idx = self._select_indexer(target_name)
        idx = idx(build_doc, indexer_env_, target_name, index_name)
        job = idx.index(self.job_manager, ids=ids, **kwargs)
        job = asyncio.ensure_future(job)

        return job

    # TODO PENDING VERIFICATION
    def update_metadata(self,
                        indexer_env,
                        index_name,
                        build_name=None,
                        _meta=None):
        """
        Update _meta for index_name, based on build_name (_meta directly
        taken from the src_build document) or _meta
        """
        idxkwargs = self[indexer_env]
        # 1st pass we get the doc_type (don't want to ask that on the signature...)
        indexer = create_backend((idxkwargs["es_host"], index_name, None)).target_esidxer
        m = indexer._es.indices.get_mapping(index_name)
        assert len(m[index_name]["mappings"]) == 1, "Found more than one doc_type: " + \
            "%s" % m[index_name]["mappings"].keys()
        doc_type = list(m[index_name]["mappings"].keys())[0]
        # 2nd pass to re-create correct indexer
        indexer = create_backend((idxkwargs["es_host"], index_name, doc_type)).target_esidxer
        if build_name:
            build = get_src_build().find_one({"_id": build_name})
            assert build, "No such build named '%s'" % build_name
            _meta = build.get("_meta")
        assert _meta is not None, "No _meta found"
        return indexer.update_mapping_meta({"_meta": _meta})

    def index_info(self, remote=False):
        """ Show index manager config with enhanced index information. """
        # http://localhost:7080/index_manager

        async def _enhance(conf):
            conf = copy.deepcopy(conf)
            if remote:
                for env in self.register:
                    try:
                        client = AsyncElasticsearch(**self.register[env]["args"])
                        conf["env"][env]["index"] = [{
                            "index": k,
                            "aliases": list(v["aliases"].keys()),
                        } for k, v in (await client.indices.get("*")).items()]

                    except Exception as exc:
                        self.logger.warning(str(exc))
                    finally:
                        try:
                            await client.close()
                        except:
                            ...

            return conf

        job = asyncio.ensure_future(_enhance(self._config))
        job.add_done_callback(self.logger.info)
        return job

    def validate_mapping(self, mapping, env):

        indexer = self._select_indexer()  # get the default indexer
        indexer = indexer(dict(mapping=mapping), self[env], None, None)

        self.logger.debug(indexer.es_client_args)
        self.logger.debug(indexer.es_index_settings)
        self.logger.debug(indexer.es_index_mappings)

        @asyncio.coroutine
        def _validate_mapping():
            client = AsyncElasticsearch(**indexer.es_client_args)
            index_name = ("hub_tmp_%s" % get_random_string()).lower()
            try:
                return (yield from client.indices.create(index_name, body={
                    "settings": (yield from indexer.es_index_settings.finalize(client)),
                    "mappings": (yield from indexer.es_index_mappings.finalize(client))
                }))
            finally:
                yield from client.indices.delete(index_name, ignore_unavailable=True)
                yield from client.close()

        job = asyncio.ensure_future(_validate_mapping())
        job.add_done_callback(self.logger.info)
        return job


class DynamicIndexerFactory():
    """
    In the context of autohub/standalone instances, create indexer
    with parameters taken from versions.json URL.
    A list of  URLs is provided so the factory knows how to create these
    indexers for each URLs. There's no way to "guess" an ES host from a URL,
    so this parameter must be specified as well, common to all URLs
    "suffix" param is added at the end of index names.
    """

    def __init__(self, urls, es_host, suffix="_current"):
        self.urls = urls
        self.es_host = es_host
        self.bynames = {}
        for url in urls:
            if isinstance(url, dict):
                name = url["name"]
                # actual_url = url["url"]
            else:
                name = os.path.basename(os.path.dirname(url))
                # actual_url = url
            self.bynames[name] = {
                "es_host": self.es_host,
                "index": name + suffix
            }

    def create(self, name):
        conf = self.bynames[name]
        pidxr = partial(ESIndexer, index=conf["index"],
                        doc_type=None,
                        es_host=conf["es_host"])
        conf = {"es_host": conf["es_host"], "index": conf["index"]}
        return pidxr, conf
