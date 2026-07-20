# 6-PACING

## Result

- Context network commands acquire both main archive locks and use one worker.
- SQLite persists the global next-request boundary before each focal request.
- The pinned runner emits only non-secret rate reset/remaining values so the
  outer worker preserves a low-quota boundary across extractor processes.
- Transient retries use bounded exponential backoff with jitter. Authentication
  and account-lock evidence stop the worker globally.
- SIGINT and SIGTERM return the lease to retryable state while retaining the
  persistent wait boundary.

## Evidence

- Fake-clock restart/reset, low-quota runner, 429 behavior, authentication,
  redaction, interrupt, retry bound, and shared-lock tests pass.

