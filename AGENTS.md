# Agent work agreement

## Language

Use Chinese when communicating with the user. Write source code, comments,
diagnostics, tests, scripts, and configuration files in English, including GitHub
Actions workflows. Standalone documents under `docs/` may remain in Chinese;
configuration examples under that directory must use English.
Preserve historical acceptance attachments under `docs/acceptance/data/` in their
original language so that archived evidence and hashes remain unchanged.

## Agent skills

### Issue tracker

This repository uses GitHub Issues for work items. Use the `gh` CLI as described in `docs/agents/issue-tracker.md`.

### Triage labels

Use the five default triage labels: `needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, and `wontfix`. See `docs/agents/triage-labels.md`.

### Domain documentation

Use the single-context layout. Read `CONTEXT.md` and `docs/adr/` as described in `docs/agents/domain.md`.
