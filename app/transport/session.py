"""Bounded entity/media caches for Telethon's in-memory encrypted-session adapter."""

from collections import OrderedDict

from telethon.sessions import StringSession


class BoundedStringSession(StringSession):
    def __init__(self, string=None, entity_limit=1000, file_limit=128):
        super().__init__(string)
        self.entity_limit = entity_limit
        self.file_limit = file_limit
        self._recent_entities = OrderedDict()

    def process_entities(self, tlo):
        # MemorySession otherwise retains every encountered peer and every past
        # username/access-hash tuple forever, even after client cache flushes.
        for entity in self._entities_to_rows(tlo):
            self._recent_entities[entity[0]] = entity
            self._recent_entities.move_to_end(entity[0])
        while len(self._recent_entities) > self.entity_limit:
            self._recent_entities.popitem(last=False)
        self._entities = set(self._recent_entities.values())

    def cache_file(self, md5_digest, file_size, instance):
        super().cache_file(md5_digest, file_size, instance)
        while len(self._files) > self.file_limit:
            self._files.pop(next(iter(self._files)))
