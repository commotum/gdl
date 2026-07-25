# 2-TELEMETRY

## Result

- `ProgressTracker` captures an immutable invocation baseline, bounded rolling
  samples, phase/activity events, lifetime totals, and deltas.
- Snapshots are strict, atomic, private (`0600`), and finalized on success,
  failure, or interruption. Final telemetry is also embedded in the invocation
  record.
- Context callbacks fire only after claims or durable outcomes and suppress
  routine pacing prose when a progress sink is active. The default no-sink
  behavior and every state transition remain unchanged.
- `SafeProgressTracker` permanently disables itself after any reporting error,
  so observability cannot change archive execution.
- Focused orchestration/context tests and the full suite pass.

## Safety Evidence

- The renderer never opens SQLite.
- Metric collection uses `mode=ro` and `query_only`.
- Reporting performs no X request, lease, cursor, or legacy-window operation.
- Engine APIs retain no-op defaults for standalone callers.
