# 7-RECOVERY

## Result

- Metadata and media leases are independently reclaimed after a conservative
  timeout. Durable observations cannot be deleted from captured targets.
- Deleted, private, suspended, and withheld evidence is terminal; transient
  and ambiguous errors are retained and bounded; exhausted work becomes
  manual review.
- `retry` explicitly requeues selected metadata targets and `retry --media`
  independently requeues context assets.
- Error evidence is size bounded and lines containing cookie/auth material are
  redacted.

## Evidence

- Stale lease, transaction fault, state trigger, classifier, retry, sensitive
  log, and database integrity tests pass.

