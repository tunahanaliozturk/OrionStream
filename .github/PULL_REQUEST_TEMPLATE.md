## What changed

<!-- One conceptual change. If this needs a bulleted list of unrelated items, it should be several PRs. -->

## Why

<!-- The problem this solves, and why it is solved here rather than somewhere else. Link the issue if there is one. -->

## How it was verified

<!-- The commands you ran and what they printed. "Tests pass" is not a verification. -->

- [ ] `dotnet build Moongazing.OrionStream.sln -c Release` — 0 errors, no new warnings
- [ ] `dotnet test Moongazing.OrionStream.sln -c Release` — green (the Redis tests need Docker)
- [ ] New behaviour has tests; a bug fix has a test that fails before the change and passes after it

## Changelog entry

<!-- The exact line to land under ## [Unreleased] in CHANGELOG.md, in the right subsection.
     Write it for someone upgrading. "n/a" only for changes with no user-visible effect. -->

```
### Fixed
- `OrionStream` (or `OrionStream.Redis`): ...
```
