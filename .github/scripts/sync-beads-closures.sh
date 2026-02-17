#!/usr/bin/env bash
set -euo pipefail

if [[ ! -f "${GITHUB_EVENT_PATH:-}" ]]; then
  echo "GITHUB_EVENT_PATH is missing; skipping."
  exit 0
fi

if [[ ! -f ".beads/issues.jsonl" ]]; then
  echo ".beads/issues.jsonl not found on this branch; skipping."
  exit 0
fi

action="$(jq -r '.action // ""' "$GITHUB_EVENT_PATH")"
merged="$(jq -r '.pull_request.merged // false' "$GITHUB_EVENT_PATH")"
if [[ "$action" != "closed" || "$merged" != "true" ]]; then
  echo "PR is not merged+closed; skipping."
  exit 0
fi

pr_number="$(jq -r '.pull_request.number // empty' "$GITHUB_EVENT_PATH")"
pr_url="$(jq -r '.pull_request.html_url // empty' "$GITHUB_EVENT_PATH")"
pr_title="$(jq -r '.pull_request.title // ""' "$GITHUB_EVENT_PATH")"
pr_body="$(jq -r '.pull_request.body // ""' "$GITHUB_EVENT_PATH")"

text_to_scan="${pr_title}"$'\n'"${pr_body}"
issue_numbers="$(
  printf '%s' "$text_to_scan" \
    | grep -Eio '(close[sd]?|fix(e[sd])?|resolve[sd]?)[:[:space:]]+#[0-9]+' \
    | grep -Eo '[0-9]+' \
    | sort -u \
    || true
)"

if [[ -z "$issue_numbers" ]]; then
  echo "No closing issue references found in PR #${pr_number}."
  exit 0
fi

timestamp="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
beads_changed=0

for issue_number in $issue_numbers; do
  echo "Syncing GitHub issue #${issue_number}"

  gh issue close "$issue_number" \
    --reason completed \
    --comment "Closed automatically by merged PR #${pr_number} (${pr_url})." \
    || true

  ref="gh-${issue_number}"
  reason="Closed via merged PR #${pr_number} (${pr_url}) for GitHub issue #${issue_number}"
  tmp_file="$(mktemp)"

  jq -c \
    --arg ref "$ref" \
    --arg ts "$timestamp" \
    --arg reason "$reason" \
    '
      if ((.external_ref // "") == $ref) and ((.status // "") != "closed") then
        .status = "closed"
        | .updated_at = $ts
        | .closed_at = $ts
        | .close_reason = $reason
      else
        .
      end
    ' .beads/issues.jsonl > "$tmp_file"

  if ! cmp -s "$tmp_file" ".beads/issues.jsonl"; then
    mv "$tmp_file" ".beads/issues.jsonl"
    beads_changed=1
  else
    rm -f "$tmp_file"
  fi
done

if [[ "$beads_changed" -eq 0 ]]; then
  echo "No Beads records required updates."
  exit 0
fi

git config user.name "github-actions[bot]"
git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
git add .beads/issues.jsonl
git commit -m "chore(beads): sync closures for merged PR #${pr_number}"
git push

