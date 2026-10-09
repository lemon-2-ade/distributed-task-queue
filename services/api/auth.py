"""
API key authentication (Phase 23).

## What this protects, and what it deliberately doesn't

`config.py`'s `api_key` setting has existed since Phase 2 but nothing
ever checked it -- every route has been open to anyone who could
reach the API process. This wires it in as a FastAPI dependency,
applied at the router level to `/tasks/*` and `/workers/*`
(`services/api/main.py`'s `include_router` calls pass
`dependencies=[Depends(require_api_key)]` for both), so every route
on those two routers requires a valid `X-API-Key` header.

`/health`, `/ready`, and `/metrics` stay open, on purpose, not by
oversight:

- `/health` and `/ready` are liveness/readiness probes
  (`services/api/routers/health.py`) -- an orchestrator's health
  check has no credential to send, and gating it behind one would
  mean a misconfigured or rotated API key takes the process out of
  rotation instead of just blocking real traffic, which is a worse
  failure mode than leaving two endpoints that reveal nothing but a
  status string unauthenticated.
- `/metrics` is scraped by Prometheus (`docker-compose.yml`), which
  likewise sends no API key. A real production deployment would put
  a network boundary in front of this (Prometheus reaching it over a
  private network/mesh, not the public internet) rather than layering
  this project's single shared secret onto a scraper -- see
  docs/security.md for the full reasoning and what's out of scope.

## Why a single shared secret, not per-caller credentials

Same honest-scoping decision `coordination/rate_limiter.py` already
documents for rate limiting: this project has no concept of multiple
distinct callers, so there is nothing for a "per-client" credential
to distinguish between. One shared `X-API-Key` is the real thing a
single-secret system can offer today -- it stops an *unauthenticated*
caller from reaching the API at all, which is the gap this phase
closes, without pretending to offer per-caller authorization it has
no identity model to back up. A genuine multi-tenant credential
scheme (API keys issued per caller, scoped permissions) is a natural
extension once something gives each caller its own identity, same as
the rate limiter's own documented next step.

## Why constant-time comparison

A naive `provided == settings.api_key` short-circuits on the first
mismatched character, which leaks (via response timing) how many
leading characters were correct -- a real, if slow, way to brute-force
a secret one character at a time. `secrets.compare_digest` is the
standard library's constant-time comparison specifically for this
class of secret-matching, so the comparison here uses it instead of
`==`.
"""

import secrets

from fastapi import Depends, HTTPException, status
from fastapi.security import APIKeyHeader

from config import get_settings

_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def require_api_key(provided_key: str | None = Depends(_api_key_header)) -> None:
    settings = get_settings()
    if provided_key is None or not secrets.compare_digest(provided_key, settings.api_key):
        # 401, not 403: no valid credential was presented at all (or
        # none was presented), which is "who are you?", not "I know
        # who you are and you're not allowed" -- the standard
        # distinction between 401 Unauthorized and 403 Forbidden.
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="missing or invalid API key",
            headers={"WWW-Authenticate": "API-Key"},
        )
