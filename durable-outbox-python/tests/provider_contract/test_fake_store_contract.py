import pytest

from durable_outbox.testing import FakeOutboxStore
from durable_outbox.testing.provider_contract import run_basic_provider_contract


@pytest.mark.asyncio
async def test_fake_store_provider_contract() -> None:
    await run_basic_provider_contract(FakeOutboxStore)
