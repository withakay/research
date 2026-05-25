from hmac import compare_digest


def claim_token_matches(current: str | None, expected: str) -> bool:
    return compare_digest(current or "", expected)
