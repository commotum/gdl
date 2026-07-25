# 5-TMUX

## Result

- Interactive tmux runs at 80x24 or larger automatically create one nine-line
  dashboard pane. The pane title includes the invocation ID and its command
  exits on final snapshot status.
- Commands use argument vectors and exact returned pane IDs. No existing pane
  is killed, resized, selected for output, or reused.
- `ARCHIVE_X_DASHBOARD=off`, small terminals, non-TTY output, missing tmux, and
  tmux command failure all fall back without affecting the worker.
- Mocked lifecycle tests prove exact targeting and no shell command string.
  No command was run against the user's live tmux server.
