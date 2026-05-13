#!/usr/bin/env bash

set -euo pipefail
set -f

usage() {
  local exit_code="${1:-2}"
  local stream="/dev/stderr"
  if [[ "$exit_code" -eq 0 ]]; then
    stream="/dev/stdout"
  fi

  cat >"$stream" <<'EOF'
Usage:
  committer [--fast] [--body BODY] [--body-file FILE] [--delete-stale-git-lock] SUBJECT [--] PATH...

Commit helper. When installed globally, delegates to repo-local scripts/committer
when present. Otherwise it stages only listed paths and commits them.
EOF
  exit "$exit_code"
}

requested_help=false
if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  requested_help=true
fi
repo_root="$(git rev-parse --show-toplevel 2>/dev/null || true)"
if [[ -z "$repo_root" ]]; then
  if [[ "$requested_help" == true ]]; then
    usage 0
  fi
  echo "error: committer must run inside a git repository" >&2
  exit 1
fi

self_path="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"
repo_committer="$repo_root/scripts/committer"
if [[ "${COMMITTER_NO_DELEGATE:-}" != "1" && -f "$repo_committer" ]]; then
  repo_committer_path="$(cd "$(dirname "$repo_committer")" && pwd)/$(basename "$repo_committer")"
  if [[ "$repo_committer_path" != "$self_path" ]]; then
    if [[ -x "$repo_committer" ]]; then
      exec "$repo_committer" "$@"
    fi
    exec bash "$repo_committer" "$@"
  fi
fi

fast_commit=false
delete_stale_git_lock=false
subject=""
body=""
body_file=""
files=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --help|-h)
      usage 0
      ;;
    --fast)
      fast_commit=true
      shift
      ;;
    --delete-stale-git-lock)
      delete_stale_git_lock=true
      shift
      ;;
    --force)
      echo "warning: --force is deprecated; use --delete-stale-git-lock" >&2
      delete_stale_git_lock=true
      shift
      ;;
    --body|-b)
      if [[ $# -lt 2 ]]; then
        echo "error: missing argument for $1" >&2
        usage
      fi
      body="$2"
      shift 2
      ;;
    --body-file)
      if [[ $# -lt 2 ]]; then
        echo "error: missing argument for --body-file" >&2
        usage
      fi
      body_file="$2"
      shift 2
      ;;
    --)
      shift
      break
      ;;
    -*)
      echo "error: unknown option: $1" >&2
      usage
      ;;
    *)
      subject="$1"
      shift
      break
      ;;
  esac
done

if [[ -z "$subject" ]]; then
  usage
fi

if [[ "$subject" != *[![:space:]]* ]]; then
  echo "error: commit subject must not be empty" >&2
  exit 1
fi

if [[ -e "$subject" ]]; then
  printf 'error: first non-option argument looks like a path: %s\n' "$subject" >&2
  echo "error: provide the commit subject first" >&2
  exit 1
fi

if [[ -n "$body" && -n "$body_file" ]]; then
  echo "error: use either --body or --body-file, not both" >&2
  exit 1
fi

if [[ -n "$body_file" && ! -f "$body_file" ]]; then
  printf 'error: body file not found: %s\n' "$body_file" >&2
  exit 1
fi

append_file_arg() {
  local raw="$1"

  if [[ "$raw" == *$'\n'* || "$raw" == *$'\r'* ]]; then
    local normalized="${raw//$'\r'/}"
    while IFS= read -r line; do
      if [[ "$line" == *[![:space:]]* ]]; then
        files+=("$line")
      fi
    done <<<"$normalized"
    return
  fi

  files+=("$raw")
}

if [[ "${1:-}" == "--" ]]; then
  shift
fi

for raw_arg in "$@"; do
  append_file_arg "$raw_arg"
done

if [[ "${#files[@]}" -eq 0 ]]; then
  usage
fi

path_exists_or_tracked() {
  local candidate="$1"
  [[ -e "$candidate" ]] || git ls-files --error-unmatch -- "$candidate" >/dev/null 2>&1
}

for file in "${files[@]}"; do
  if [[ "$file" == "." ]]; then
    echo 'error: "." is not allowed; list specific paths instead' >&2
    exit 1
  fi

  case "$file" in
    *node_modules* | */node_modules | */node_modules/* | node_modules)
      printf 'error: node_modules paths are not allowed: %s\n' "$file" >&2
      exit 1
      ;;
  esac

  if ! path_exists_or_tracked "$file"; then
    printf 'error: file not found: %s\n' "$file" >&2
    exit 1
  fi
done

last_git_error=""

run_git_command() {
  local stderr_log
  stderr_log="$(mktemp)"

  if "$@" 2>"$stderr_log"; then
    if [[ -s "$stderr_log" ]]; then
      cat "$stderr_log" >&2
    fi
    rm -f "$stderr_log"
    last_git_error=""
    return 0
  fi

  if [[ -s "$stderr_log" ]]; then
    cat "$stderr_log" >&2
  fi
  last_git_error="$(cat "$stderr_log")"
  rm -f "$stderr_log"
  return 1
}

is_git_lock_error() {
  printf '%s\n' "$last_git_error" | grep -Eq \
    "Another git process seems to be running|Unable to create '.*\\.git/[^']+\\.lock'"
}

extract_git_lock_path() {
  printf '%s\n' "$last_git_error" |
    sed -n "s/.*'\(.*\.git\/[^']*\.lock\)'.*/\1/p" |
    head -n 1
}

run_git_with_lock_retry() {
  local label="$1"
  shift

  local deadline=$((SECONDS + 5))
  local announced_retry=false

  while true; do
    if run_git_command "$@"; then
      return 0
    fi

    if ! is_git_lock_error; then
      return 1
    fi

    if [[ "$SECONDS" -ge "$deadline" ]]; then
      break
    fi

    if [[ "$announced_retry" == false ]]; then
      printf 'git lock during %s; retrying for up to 5 seconds...\n' "$label" >&2
      announced_retry=true
    fi

    sleep 0.5
  done

  if [[ "$delete_stale_git_lock" == true ]]; then
    local lock_path
    lock_path="$(extract_git_lock_path)"
    if [[ -n "$lock_path" && -e "$lock_path" ]]; then
      rm -f "$lock_path"
      printf 'removed stale git lock: %s\n' "$lock_path" >&2
      run_git_command "$@"
      return $?
    fi
  fi

  return 1
}

if ! git diff --staged --quiet; then
  echo "error: existing staged changes detected; commit or unstage them first" >&2
  exit 1
fi

if git rev-parse --verify HEAD >/dev/null 2>&1; then
  run_git_with_lock_retry "unstaging files" git restore --staged :/
fi
run_git_with_lock_retry "staging files" git add --force -- "${files[@]}"

if git diff --staged --quiet; then
  echo "error: no staged changes detected for selected paths" >&2
  exit 1
fi

message_file="$(mktemp)"
trap 'rm -f "$message_file"' EXIT

printf '%s\n' "$subject" >"$message_file"
if [[ -n "$body_file" ]]; then
  printf '\n' >>"$message_file"
  cat "$body_file" >>"$message_file"
  printf '\n' >>"$message_file"
elif [[ -n "$body" ]]; then
  printf '\n%s\n' "$body" >>"$message_file"
fi

commit_cmd=(git commit -F "$message_file")
if [[ "$fast_commit" == true ]]; then
  commit_cmd=(env FAST_COMMIT=1 "${commit_cmd[@]}")
fi

run_git_with_lock_retry "commit" "${commit_cmd[@]}"
printf 'Committed "%s" with %d path(s).\n' "$subject" "${#files[@]}"
