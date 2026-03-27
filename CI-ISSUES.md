## Workflow Compatibility on a Personal Fork

### Build workflows — the core issue

**`build-scylla.yaml`** has a hard guard on line 19:

```yaml
if: github.repository == 'scylladb/scylladb'
```

This applies to `reproducible-build.yaml` and `clang-nightly.yaml` too (same guard). **These jobs will be silently skipped** on any personal fork. You cannot use them to verify the build.

**`clang-tidy.yaml` is the only workflow that can actually compile the code on a personal fork.** It has no repo guard, uses `workflow_dispatch` (manual trigger), and runs the full `cmake + ninja --target scylla` using the public toolchain image `docker.io/scylladb/scylla-toolchain:fedora-43-20260304`. Be aware: a full ScyllaDB build takes 60–90+ minutes.

---

### Workflows broken by missing Scylla-internal secrets

These will error (not skip) because secrets resolve to empty strings and `curl` calls will fail with HTTP 401:

| Workflow                                                                                   | Missing secrets                                           |
| ------------------------------------------------------------------------------------------ | --------------------------------------------------------- |
| `trigger_ci.yaml`                                                                          | `JENKINS_USERNAME`, `JENKINS_TOKEN`, `SLACK_BOT_TOKEN`    |
| `trigger-scylla-ci.yaml`                                                                   | same, plus requires `MEMBER`/`OWNER` org association      |
| `trigger_jenkins.yaml`                                                                     | same (only fires on `next**` branches anyway)             |
| `call_backport_with_jira.yaml`                                                             | `AUTO_BACKPORT_TOKEN`, `USER_AND_KEY_FOR_JIRA_AUTOMATION` |
| `call_jira_sync.yml`, `call_sync_milestone_to_jira.yml`, `call_jira_sync_pr_milestone.yml` | `USER_AND_KEY_FOR_JIRA_AUTOMATION`                        |
| `add-label-when-promoted.yaml`                                                             | `AUTO_BACKPORT_TOKEN`                                     |
| `make-pr-ready-for-review.yaml`                                                            | `AUTO_BACKPORT_TOKEN`                                     |

These are all opt-in integrations with Scylla's private Jenkins and Jira — they don't affect whether code compiles.

---

### Workflows that would run cleanly on a fork

| Workflow                            | Trigger                           | Notes                                                                                                                           |
| ----------------------------------- | --------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| `codespell.yaml`                    | PR to `master`                    | Spelling only, `only_warn: 1` — never fails                                                                                     |
| `check-license-header.yaml`         | PR to `master`                    | Our new files **pass** — `LicenseRef-ScyllaDB-Source-Available-1.0` is within the first 10 lines of all 4 new `.cc`/`.hh` files |
| `differential-shellcheck.yaml`      | push/PR to `master`               | Needs `security-events: write` — available on personal forks                                                                    |
| `docs-pr.yaml`                      | PR to `master` touching `docs/**` | Won't fire for our changes                                                                                                      |
| `backport-pr-fixes-validation.yaml` | PR to `branch-*`                  | Won't fire on `master` PRs                                                                                                      |
| `iwyu.yaml`                         | PR to `master`                    | Runs include cleaner — relevant to our code, no repo guard                                                                      |

---

### One workflow that will actively block PRs

**`pr-require-backport-label.yaml`** runs on every non-draft PR to `master` or `next` and calls `mheap/github-action-required-labels` requiring at least one of: `backport/none`, `backport/YYYY.N`, or `backport/N.N`. On a fresh fork **these labels don't exist**, so the check will fail and block merging. You'd need to create those labels in the fork's repository settings first.

---
