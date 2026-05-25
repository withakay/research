import pytest

from durable_outbox.core import ClaimConflictError, RetryableStoreError, ValidationError
from durable_outbox.core.dispatcher import _stored_error_message


@pytest.mark.parametrize(
    "error",
    [
        ClaimConflictError("claim token 11111111-2222-3333-4444-555555555555 leaked"),
        RetryableStoreError("owner=11111111-2222-3333-4444-555555555555"),
        ValidationError("bad token 11111111-2222-3333-4444-555555555555"),
    ],
)
def test_claim_token_never_in_stored_error_message(error: BaseException) -> None:
    stored = _stored_error_message(error)

    assert "11111111-2222-3333-4444-555555555555" not in stored.message
    assert "<uuid>" in stored.message
