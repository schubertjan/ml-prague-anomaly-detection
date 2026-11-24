"""ElasticSearch output."""

from __future__ import annotations

import logging

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

log = logging.getLogger(__name__)


class EsWriter:
    def __init__(self, hosts: list[str], index: str, settings: dict | None = None):
        self.hosts = hosts
        self.index = index
        self.es = Elasticsearch(hosts)
        if settings:
            self._ensure_index(settings)

    def _ensure_index(self, settings: dict) -> None:
        try:
            if not self.es.indices.exists(index=self.index):
                self.es.indices.create(index=self.index, settings=settings)
                log.info("created index %s", self.index)
        except Exception as e:  # ES down / no permission -> keep going, index may exist later
            log.warning("could not ensure index %s: %s", self.index, e)

    def write(self, docs: list[tuple[str, dict]]) -> None:
        """Bulk-write (doc_id, doc) pairs. No-op when empty."""
        if not docs:
            return
        actions = [{"_index": self.index, "_id": doc_id, "_source": doc} for doc_id, doc in docs]
        ok, errors = bulk(self.es, actions, raise_on_error=False)
        if isinstance(errors, list) and errors:
            log.error("bulk write errors: %s", errors[:3])
        if ok:
            log.info("wrote %d docs to %s", ok, self.index)
