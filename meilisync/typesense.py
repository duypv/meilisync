import asyncio
from typing import AsyncGenerator, List, Optional, Type, Union

import typesense

from meilisync.enums import EventType
from meilisync.event import EventCollection
from meilisync.plugin import Plugin
from meilisync.schemas import Event
from meilisync.settings import Sync


class Typesense:
    def __init__(
        self,
        host: str,
        port: int,
        protocol: str,
        api_key: str,
        plugins: Optional[List[Union[Type[Plugin], Plugin]]] = None,
        connection_timeout_seconds: int = 10,
    ):
        self.client = typesense.Client(
            {
                "nodes": [
                    {
                        "host": host,
                        "port": str(port),
                        "protocol": protocol,
                    }
                ],
                "api_key": api_key,
                "connection_timeout_seconds": connection_timeout_seconds,
            }
        )
        self.plugins = plugins or []

    @staticmethod
    def _is_not_found_error(error: Exception):
        status = getattr(error, "http_status", None) or getattr(error, "status", None)
        if status == 404:
            return True
        return "not found" in str(error).lower()

    async def _create_collection(self, index_name: str):
        schema = {
            "name": index_name,
            "enable_nested_fields": True,
            "fields": [
                {
                    "name": ".*",
                    "type": "auto",
                }
            ],
        }
        await asyncio.to_thread(self.client.collections.create, schema)

    async def index_exists(self, index: str):
        try:
            await asyncio.to_thread(self.client.collections[index].retrieve)
            return True
        except Exception as error:
            if self._is_not_found_error(error):
                return False
            raise

    async def ensure_collection(self, index_name: str):
        if not await self.index_exists(index_name):
            await self._create_collection(index_name)

    async def add_data(self, sync: Sync, data: list):
        events = [Event(type=EventType.create, data=item) for item in data]
        return await self.handle_events_by_type(sync, events, EventType.create)

    async def refresh_data(self, sync: Sync, data: AsyncGenerator):
        index_name = sync.index_name
        if await self.index_exists(index_name):
            await asyncio.to_thread(self.client.collections[index_name].delete)
        count = 0
        async for items in data:
            await self.add_data(sync, items)
            count += len(items)
        return count

    async def get_count(self, index: str):
        if not await self.index_exists(index):
            return 0
        result = await asyncio.to_thread(self.client.collections[index].retrieve)
        return result.get("num_documents", 0)

    async def handle_events(self, collection: EventCollection):
        created_events, updated_events, deleted_events = collection.pop_events
        for sync, events in created_events.items():
            await self.handle_events_by_type(sync, events, EventType.create)
        for sync, events in updated_events.items():
            await self.handle_events_by_type(sync, events, EventType.update)
        for sync, events in deleted_events.items():
            await self.handle_events_by_type(sync, events, EventType.delete)

    async def handle_plugins_pre(self, sync: Sync, event: Event):
        for plugin in self.plugins:
            if isinstance(plugin, Plugin):
                event = await plugin.pre_event(event)
            else:
                event = await plugin().pre_event(event)
        for plugin in sync.plugins_cls():
            if isinstance(plugin, Plugin):
                event = await plugin.pre_event(event)
            else:
                event = await plugin().pre_event(event)
        return event

    async def handle_plugins_post(self, sync: Sync, event: Event):
        for plugin in self.plugins:
            if isinstance(plugin, Plugin):
                event = await plugin.post_event(event)
            else:
                event = await plugin().post_event(event)
        for plugin in sync.plugins_cls():
            if isinstance(plugin, Plugin):
                event = await plugin.post_event(event)
            else:
                event = await plugin().post_event(event)
        return event

    async def handle_events_by_type(self, sync: Sync, events: List[Event], event_type: EventType):
        if not events:
            return
        await self.ensure_collection(sync.index_name)
        collection = self.client.collections[sync.index_name]
        processed_events = []
        for event in events:
            processed_events.append(await self.handle_plugins_pre(sync, event))
        if event_type in (EventType.create, EventType.update):
            documents = []
            for event in processed_events:
                payload = event.mapping_data(sync.fields)
                payload["id"] = str(event.data[sync.pk])
                documents.append(payload)
            await asyncio.to_thread(collection.documents.import_, documents, {"action": "upsert"})
        elif event_type == EventType.delete:
            for event in processed_events:
                await asyncio.to_thread(collection.documents[str(event.data[sync.pk])].delete)
        for event in processed_events:
            await self.handle_plugins_post(sync, event)

    async def handle_event(self, event: Event, sync: Sync):
        event = await self.handle_plugins_pre(sync, event)
        await self.ensure_collection(sync.index_name)
        collection = self.client.collections[sync.index_name]
        if event.type in (EventType.create, EventType.update):
            payload = event.mapping_data(sync.fields)
            payload["id"] = str(event.data[sync.pk])
            await asyncio.to_thread(collection.documents.import_, [payload], {"action": "upsert"})
        elif event.type == EventType.delete:
            await asyncio.to_thread(collection.documents[str(event.data[sync.pk])].delete)
        await self.handle_plugins_post(sync, event)
