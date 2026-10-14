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
