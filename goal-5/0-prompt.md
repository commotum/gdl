# Goal 5 Continuation Prompt

```text
Work through /home/jake/Developer/gdl/goal-5/0-plan.md using the repeatable
protocol, invariants, ledgers, and stage template in
/home/jake/Developer/gdl/goal-5/0-loop.md.

Objective: make `uv run scripts/archive-x --user USERNAME` a materially faster,
lower-footprint unified archiver without weakening archive safety. Reuse the
first accepted extraction's media descriptors for context, authored, legacy,
and profile assets; make direct CDN transfer the normal path; reserve exact X
post refresh for bounded missing/stale-descriptor cases; pace every actual X
call through one durable restart-safe account lane; reduce repeated
process/session/bootstrap work when proven safe; make legacy windows adaptive
while retaining two independent matching observations; and replace
whole-history hot-path scans/rewrites with incremental indexed commits plus
coherent reproducible exports.

Follow the stage order: 1-MEASURE, 2-STATE, 3-DESCRIPTORS, 4-DIRECT-MEDIA,
5-FALLBACKS, 6-PACING, 7-LEGACY, 8-LOCAL, 9-INTEGRATE, 10-PROVE. Start by
syncing actual code, tests, git status, existing stage evidence, and safe
read-only runtime facts. Correct 0-plan.md if measured evidence differs. Select
the first incomplete stage, create its stage file, define its baseline and
no-cheating verifier before implementation, implement only that stage, run the
verification ladder, record before/after results and rejected alternatives,
and fold authoritative evidence back into 0-plan.md before continuing.

Critical measurement rule: count actual HTTP/API boundaries, not subprocesses,
targets, or `fetch_post()` calls. Report X API, X bootstrap/support/profile,
CDN, redirect, and retry calls separately, along with runner starts, call
spacing/concurrency, descriptor hits/refreshes, legacy windows/walks, bytes
hashed/read/written, materialization generations, query plans, files/outcomes,
and wall time. Never expose full URLs/query strings, cookies, headers, signed
tokens, or private descriptor data.

Network invariants: keep the mandatory numeric-ID identity guard; at most one X
request may be in flight; persist not-before/reset state across phases and
restart; honor rate limits, 429s, auth stops, locks, and challenges; prevent
catch-up bursts; do not shorten stacked sleeps until actual-call pacing is
proven; permit only bounded CDN overlap; use no paid API/proxy/service and no
fingerprint, cookie, identity, header, CAPTCHA, or proxy evasion. The goal is
fewer, predictable, service-friendly requests—not a promise of
"undetectability."

Archive invariants: persist descriptors only for accepted/canonical post IDs;
never download every nearby conversation/search result; keep metadata/frontier
commits independent of media; perform no network/file I/O inside SQLite write
transactions; preserve focal/identity/bounds/depth/cycle/cursor guards; retain
two independent matching valid legacy observations, exact UTC intervals,
request caps, empty-tail proof, numeric record validation, and safe splitting;
preserve raw snapshots, restrictive modes, atomic files, hashes, sidecars,
download-ledger compatibility, durable retries, unavailable/manual-review
truth, low-disk and Ctrl-C behavior.

Efficiency invariants: a usable context or shared-media descriptor causes zero
extra X tweet lookup; unchanged profile descriptors cause no avatar/background
X extractor or CDN transfer; matching valid legacy evidence survives an
intervening transient invalid attempt and restart; ordinary small deltas do not
rebuild all historical JSONL; K legacy windows do not export K times; unchanged
sources are not rehashed/reparsed in ordinary seeding; unchanged generations
are not re-exported; hot queue claims do not full-scan/temp-sort all targets;
explicit full integrity audit and portable export remain available.

Treat all P0 work as mandatory. Implement each P1 item or record measurements,
safety analysis, and a plan amendment proving why it has no material benefit or
cannot be made equivalently safe. Implement P2 startup/dashboard/recovery work
only if the measured threshold in the plan is crossed. Do not hide a skipped
high-cost path behind compatibility fallback.

Preserve all pre-existing dirty/unrelated user work. Do not start, stop, resume,
or mutate a production archive for testing. A bounded live smoke requires
explicit authorization after fixture, migration, fault, request-ledger,
query-plan, performance, full-suite, fingerprint, and diff proof.

Completion means the original Goal 5 objective and every required success
metric are directly proven: fewer actual X calls, no redundant happy-path media
lookups, no increased X request density/concurrency, materially fewer runner
starts where safe, lower legacy request cost with equivalent evidence, at least
90% lower ordinary I/O on the large small-delta fixture, correct migration and
exports, and unchanged archive safety under the same one-command interface. Do
not declare success by renaming phases, hiding calls in a persistent worker,
moving full rewrites elsewhere, or silently routing new work through old exact
post fallback.
```
