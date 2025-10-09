# CryptoKids Ledger Boundary Contract

This document defines the server contract for all ledger verbs. Every verb is
exposed as an HTTP endpoint under the `/ck` namespace. Responses are JSON
objects that **always include** the current `balance`, a `hints` object with
state flags, and a `txId` (when a transaction is recorded). Unless stated
otherwise all endpoints require the `X-Admin-Key` header and expect the caller
to include `X-Actor-Role` (either `admin` or `parent`). Idempotent calls may
provide an `Idempotency-Key` header.

## Shared response fields

* `balance` – integer point balance after the verb is applied.
* `hints` – server-computed state object with:
  * `balance`, `can_redeem`, `max_redeem`, `can_refund`, `max_refund`,
    `max_redeem_for_reward`, `hold_status`, `pending_hold_count`,
    `refundable_redeems[]`, and `features` (currently `{ refunds: boolean }`).
* `txId` – UUID string of the ledger transaction when one is created.
* `tx` – full ledger row for the new posting (when applicable).

Errors return `error` codes (upper snake case) alongside `balance`/`hints`
whenever the user can be resolved.

## Earn – `POST /ck/earn`

Credit points to a member account. Requires `admin` role.

* Required body: `user_id`, `amount` (positive integer)
* Optional body: `note`, `action`, `idempotency_key`
* Errors: `INVALID_AMOUNT`, `INVALID_USER`, `EARN_FAILED`

## Redeem – `POST /ck/redeem`

Debit points directly from a member account (without a hold). Requires
`admin` role and enforces overdraft protection.

* Required: `user_id`, `amount`
* Optional: `note`, `action`, `idempotency_key`
* Errors: `INVALID_AMOUNT`, `INVALID_USER`, `INSUFFICIENT_FUNDS`,
  `REDEEM_FAILED`

## Adjust – `POST /ck/adjust`

Manual balance adjustments. Positive values credit, negative values debit.
Requires `admin` role.

* Required: `user_id`, `delta` (non-zero integer)
* Optional: `note`, `action`, `idempotency_key`
* Errors: `INVALID_DELTA`, `INVALID_USER`, `INSUFFICIENT_FUNDS`,
  `ADJUST_FAILED`

## Refund – `POST /ck/refund`

Return points to a member by referencing a redeemed transaction. Enforces the
`CK_REFUND_WINDOW_DAYS`, over-refund guards, idempotency, and rate limits.
Requires `admin` role and the feature flag `FEATURE_REFUNDS`.

* Required: `user_id`, `redeem_tx_id`, `amount`, `reason`
* Optional: `notes`, `idempotency_key`
* Errors: `FEATURE_DISABLED`, `INVALID_AMOUNT`, `REDEEM_NOT_FOUND`,
  `USER_MISMATCH`, `NOT_REDEEM_TX`, `REFUND_NOT_ALLOWED`, `OVER_REFUND`,
  `INVALID_REASON`, `REFUND_EXISTS`, `IDEMPOTENCY_CONFLICT`,
  `REFUND_WINDOW_EXPIRED`, `ROLE_REQUIRED`

## Hold Reserve – `POST /api/holds`

Creates a pending hold for a reward and returns a spend token. Does not require
an admin key (child initiated) but the response still includes ledger hints.

* Required: `userId`, `itemId`
* Errors: `invalid_payload`, `reward_not_found`, `hold_failed`

## Hold Release – `POST /api/holds/:id/cancel`

Cancels a pending hold and records an informational ledger entry. Requires
`admin` role via `X-Actor-Role` header.

* Errors: `hold_not_pending`, `Cancel failed`

## Hold Redeem – `POST /api/holds/:id/approve`

Approves a hold using a spend token. Deducts points atomically and returns
updated hints.

* Required body: `token`
* Optional: `finalCost`
* Errors: `invalid_payload`, `unsupported_token`, `hold_mismatch`,
  `TOKEN_USED`, `hold_not_pending`, `approve_failed`

## Earn via QR – `POST /api/earn/scan`

Redeems earn or give QR tokens. Uses token idempotency and returns hints.

* Required body: `token`
* Errors: `missing_token`, `unsupported_token`, `TOKEN_USED`,
  `ADMIN_REQUIRED`, `scan_failed`

## Quick Earn – `POST /api/earn/quick`

Awards a template-defined earn to a member. Returns hints for the affected user.

* Required: `userId`, `templateId`
* Errors: `invalid_payload`, `template_not_found`, `EARN_FAILED`

## Holds API – `GET /api/holds`

Lists holds filtered by status. Responses are plain arrays (no hints) because
they may span multiple users. Use the per-verb responses above to refresh
state hints after a mutation.

## History – `GET /api/history`

Contract testing endpoint used by the Activity view. Supports filters via
query parameters: `userId`, `verb`, `actor`, `from`, `to`, `limit`, `offset`.
Returns `{ rows: LedgerRow[], limit, offset }` where each row matches the
ledger schema (including `parent_tx_id`).

---

All endpoints emit telemetry internally (verb success/failure counts and top
error codes) and are surfaced through `GET /api/admin/telemetry/core-health`.
