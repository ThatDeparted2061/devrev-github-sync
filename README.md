**10,023 changes replayed through GitHub and DevRev, with 19% of webhooks delivered twice, 5% never delivered and all of them out of order, end with zero duplicate work items, zero lost updates, and every drift healed within 12 seconds of the last change.**

# devrev-github-sync

[![CI](https://github.com/ThatDeparted2061/devrev-github-sync/actions/workflows/ci.yml/badge.svg)](https://github.com/ThatDeparted2061/devrev-github-sync/actions/workflows/ci.yml)

A two-way sync between GitHub and [DevRev](https://devrev.ai), built on DevRev's public API, that stays correct when webhooks are duplicated, dropped or delivered out of order.

- GitHub issues and pull requests create and update DevRev work items (`works.create`, `works.update`). Status changes made in DevRev flow back to GitHub (close, reopen, with the matching `state_reason`).
- Every webhook is written to a Postgres event log, keyed on its delivery ID (`X-GitHub-Delivery`, or the DevRev event `id`). A redelivery hits the unique constraint and is dropped before any work happens.
- Workers retry failures with exponential backoff and jitter, honour `Retry-After`, and move an event to a dead-letter queue after 8 attempts (`GET /dlq`, `POST /dlq/replay`).
- A reconciler diffs both sides every N minutes and heals whatever the webhooks missed.
- Chaos mode: a simulated GitHub and DevRev deliver every webhook with duplicates, drops and random delays, and fail API calls with 429s and 5xx.

## Result

`make demo` (or `docker compose up`) drives 10,000 user changes through a simulated GitHub repository and DevRev org: 1,000 issues and PRs opened, then edits, closes, reopens and merges on GitHub, and stage changes in DevRev. Every webhook either system sends, including the echoes of the sync's own writes, has a 20% chance of being delivered twice and a 5% chance of never arriving, and waits a random 0-2 s first, so deliveries overtake each other. Both APIs answer 2% of calls with 429 and 1% with 502. The run then waits for the sync to settle and checks every object against the audit log of what users actually did.

| | |
|---|---:|
| User changes (GitHub + DevRev) | 10023 |
| Objects (issues / pull requests) | 806 / 194 |
| Webhooks generated (incl. the sync's own echoes) | 17028 |
| Delivered twice | 3237 (19.0%) |
| Never delivered | 826 (4.9%) |
| Delivered out of order | all (random 0-2 s delay each) |
| API calls answered 429 / 5xx | 820 / 406 |
| API writes that committed but answered 502 | 79 |
| Duplicate deliveries rejected by delivery ID | 3171 |
| Events coalesced into a later sync | 3366 |
| Dead-lettered events | 2 |
| **Duplicate work items** | 0 |
| **Missing work items** | 0 |
| **Lost updates** | 0 |
| Concurrent-edit races (user changed a side between the sync's read and write; see README) | 0 |
| **Drift healed within** (last change -> both sides agree) | 12.4 s |
| Converged | yes |

Three back-to-back runs (`bench/results/chaos-run{1,2,3}.json`) gave 0, 1, 0 lost updates out of about 10,000 changes, with zero duplicates and full convergence (healed in 12, 12, 12 s) every time. The single lost update was a DevRev stage change whose webhook was dropped while GitHub changed status in the same window; see *Limits*.

"Lost updates" means an object where either side ended up with a title or status different from the last change a user made. Status changes can come from either side; title changes come from GitHub.

## How it stays correct

**It syncs state, not events.** A webhook only tells the sync "object K changed". A worker takes a Postgres advisory lock on K, reads GitHub and DevRev as they are now, and makes them agree. That one decision handles all three failure modes:

| Failure | Why it is harmless |
|---|---|
| Duplicate delivery | Rejected by the `UNIQUE (source, delivery_id)` constraint. If one slips through under another ID, the second sync finds both sides already equal and does nothing. |
| Out-of-order delivery | No payload is ever applied. The worker reads the current state, so an old event cannot overwrite a new one. A GitHub read older than the webhook that triggered it (replica lag) is retried. |
| Dropped delivery | The reconciler lists both sides, finds the object that disagrees, and queues a sync for it through the same event log (with the same retries and DLQ). |

**Status is a three-way merge.** For each object, the sync stores the merge base: the status it last brought both sides to. If only GitHub moved, GitHub wins. If only DevRev moved, DevRev wins. If both moved, the later *status change* wins. GitHub's time comes from the issue events API. DevRev's comes from the stage transitions in its `work_updated` webhooks, which are already in the event log. `modified_date` is not used, because any later edit bumps it.

**History catches what snapshots miss.** If a side changes A → B → A between syncs, it looks unchanged. So before letting one side overwrite the other, the sync checks the overwritten side's history (GitHub's events API, DevRev's recorded transitions). If that history shows a change newer than the snapshot it just read, it starts over with fresh reads. If DevRev was modified but the webhook explaining it hasn't arrived, it waits up to 10 s for it (`WebhookGrace`), because DevRev has no stage-history API.

**No call inside a sync retries in place.** A sync that waited out a 429 between its reads and its writes would act on stale reads. Instead, any failed call fails the whole sync, which is retried from fresh reads, with `Retry-After` honoured at the event level. `works.create` is never retried blindly: if its response is lost after DevRev committed it, the next attempt finds the item by `external_ref`.

**Duplicates are stopped twice.** First by the per-object lock plus a `works.list?external_ref=` lookup before every create. Then by DevRev itself, since `external_ref` (`github:owner/repo#123`) is unique per work type. The benchmark turns DevRev's uniqueness check off in the simulator (`-unique-external-ref=false`), so its zero-duplicates result rests on the sync alone.

**Echoes end on their own.** When the sync closes a GitHub issue, GitHub sends a webhook back. The sync processes it, finds both sides equal, and stops. Pending events for an object that a sync has already covered are closed out without another round of API calls (the "coalesced" count).

**Ownership.** Title and body are owned by GitHub. Status is owned by both sides. DevRev cannot merge a pull request, so a PR work item marked `completed` in DevRev leaves the PR open and counts as in sync. `wont_fix` and `duplicate` close the PR.

| GitHub | DevRev stage |
|---|---|
| open issue | keeps its open stage (`triage` for new, `backlog` when reopened) |
| open PR | `in_development` |
| closed, completed / merged PR | `completed` |
| closed, not planned / closed PR | `wont_fix` |
| closed, duplicate | `duplicate` |

## Run it

```bash
make demo        # Go + local Postgres: the full 10,000-change chaos run (~1 min)
make demo-quick  # 2,000 changes
make up          # the same in docker compose (Postgres, simulator, sync, chaos driver)
make test        # unit + integration tests (needs Postgres; skipped without it)
```

Against real systems, run `cmd/sync` with the settings in [`.env.example`](.env.example):

1. GitHub: add a repository webhook to `https://<host>/webhooks/github` for the *Issues* and *Pull requests* events, with a secret.
2. DevRev: `POST https://api.devrev.ai/webhooks.create` with `{"url": "https://<host>/webhooks/devrev", "event_types": ["work_created", "work_updated"]}`. Store the returned `secret`. The sync answers the `verify` challenge itself.
3. Set `DEVREV_PART_ID` and `DEVREV_OWNER_ID` for new work items, then start the service.

| Endpoint | |
|---|---|
| `POST /webhooks/github`, `POST /webhooks/devrev` | Receivers. They verify the HMAC-SHA256 signature, record the event and answer `202` in milliseconds. |
| `GET /dlq`, `POST /dlq/replay` | Inspect and re-drive dead-lettered events |
| `POST /reconcile` | Run a reconciliation pass now |
| `GET /stats`, `GET /metrics` | Event-log counts and the last reconcile report, plus Prometheus metrics |

## Layout

| Path | |
|---|---|
| `internal/store` | Event log, idempotency, leases (`FOR UPDATE SKIP LOCKED`), DLQ, links table (`schema.sql`) |
| `internal/syncer` | `syncer.go` converge + status merge, `reconcile.go`, `server.go` webhooks, workers |
| `internal/github`, `internal/devrev` | API clients and signature checks |
| `internal/retry` | Backoff with jitter, `Retry-After`, retrying JSON client |
| `internal/sim`, `cmd/sim` | Simulated GitHub + DevRev with webhook chaos and API faults |
| `cmd/chaos` | Benchmark driver and checker |

## Notes and limits

- **A dropped DevRev status webhook in a conflict.** When DevRev's stage-change webhook never arrives and GitHub changes status in the same window, the sync has to fall back to `modified_date` to order the two changes. DevRev exposes no stage history, so that can pick the wrong winner: 1 change in about 30,000 across the three runs. DevRev's timeline entries API would be the place to recover the exact time.
- **Concurrent-edit races.** Neither GitHub's issue `PATCH` nor DevRev's `works.update` takes a version precondition. So if a person changes an object in the few milliseconds between the sync's read and its write, no sync can tell, and the person's change is overwritten. The checker counts these separately as "races": a sync write within 100 ms of a user change to the same object. The checker identifies them exactly: the simulator logs the sync's reads, and a race is a user change that lands between the sync's read of a side and its write to that side. They depend on timing, not on the chaos, and they are reported, not hidden.
- GitHub's `updated_at` has one-second resolution. The status merge does not depend on it, because only stale-read detection uses it.
- The reconciler does a full scan of both sides each pass, about 10 API pages per 1,000 objects. Large repositories should switch the GitHub side to `?since=`.
- The DevRev webhook signature encoding is not pinned in the docs, so both hex and base64 are accepted.
