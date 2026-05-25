from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import asdict
from typing import Any

from durable_outbox.core import OutboxDispatcher
from durable_outbox.sinks.kafka import KafkaProducerConfig, KafkaSink
from durable_outbox.stores.azure_blob import AzureBlobClient
from durable_outbox.stores.blob_geo import BlobOutboxStore
from fastapi import Body, FastAPI, Header, Request, Response, status
from pydantic import BaseModel

from durable_outbox_fastapi.config import AppSettings
from durable_outbox_fastapi.service import PublisherService


class PublishAccepted(BaseModel):
    event_id: str
    topic: str
    accepted_at: str
    store: str
    rpo_zero: bool
    dispatch: Mapping[str, int]


type Lifespan = Callable[[FastAPI], AbstractAsyncContextManager[None]]


def create_app(*, lifespan: Lifespan | None = None) -> FastAPI:
    app = FastAPI(
        title="durable-outbox-fastapi",
        version="0.1.0",
        lifespan=lifespan or _lifespan,
    )

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.post(
        "/topics/{topic}/messages",
        status_code=status.HTTP_202_ACCEPTED,
        response_model=PublishAccepted,
    )
    async def publish_message(
        topic: str,
        request: Request,
        response: Response,
        payload: Any = Body(...),
        x_message_key: str | None = Header(default=None),
    ) -> dict[str, Any]:
        publisher = _publisher(request.app)
        result = await publisher.publish(
            topic=topic,
            payload=payload,
            key=x_message_key,
        )
        response.headers["location"] = f"/events/{result.event_id}"
        return {
            **asdict(result),
            "accepted_at": result.accepted_at.isoformat(),
        }

    return app


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = AppSettings.from_env()
    blob_client = AzureBlobClient.from_connection_string(
        settings.blob_connection_string,
        container_name=settings.blob_container_name,
    )
    await blob_client.ensure_container()
    store = BlobOutboxStore(client=blob_client, environment="fastapi")
    sink = KafkaSink.from_config(
        KafkaProducerConfig({"bootstrap.servers": settings.kafka_bootstrap_servers})
    )
    app.state.publisher = PublisherService(
        store=store,
        dispatcher=OutboxDispatcher(store, sink),
        default_ttl=settings.default_ttl,
    )
    try:
        yield
    finally:
        sink.close()
        await blob_client.close()


def _publisher(app: FastAPI) -> PublisherService:
    publisher = getattr(app.state, "publisher", None)
    if not isinstance(publisher, PublisherService):
        raise RuntimeError("publisher service is not initialized")
    return publisher
