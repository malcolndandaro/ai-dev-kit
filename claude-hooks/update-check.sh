#!/bin/bash
# AI Dev Kit — lightweight update check (runs on SessionStart)
# Any failure is silent; never blocks the session.
trap 'exit 0' ERR

VERSION_FILE="$HOME/.ai-dev-kit/version"
CACHE_FILE="$HOME/.ai-dev-kit/.update-check"
RAW_URL="https://raw.githubusercontent.com/databricks-solutions/ai-dev-kit/main/VERSION"
CACHE_TTL=86400 # 24 hours

[ ! -f "$VERSION_FILE" ] && exit 0
local_ver=$(cat "$VERSION_FILE" 2>/dev/null) || exit 0
[ -z "$local_ver" ] && exit 0

# Check cache
remote_ver=""
if [ -f "$CACHE_FILE" ]; then
    cached_ts=$(sed -n '1p' "$CACHE_FILE" 2>/dev/null) || cached_ts=0
    cached_ver=$(sed -n '2p' "$CACHE_FILE" 2>/dev/null) || cached_ver=""
    now=$(date +%s)
    if [ $(( now - cached_ts )) -lt $CACHE_TTL ] && [ -n "$cached_ver" ]; then
        remote_ver="$cached_ver"
    fi
fi

# Fetch if cache is stale or missing
if [ -z "$remote_ver" ]; then
    remote_ver=$(curl -fsSL --max-time 2 "$RAW_URL" 2>/dev/null) || exit 0
    [[ "$remote_ver" =~ (404|Not\ Found|error) ]] && exit 0
    remote_ver=$(echo "$remote_ver" | tr -d '[:space:]')
    # Update cache
    printf '%s\n%s\n' "$(date +%s)" "$remote_ver" > "$CACHE_FILE" 2>/dev/null
fi

# Notify if versions differ
if [ -n "$remote_ver" ] && [ "$local_ver" != "$remote_ver" ]; then
    echo "AI Dev Kit $remote_ver is available (current: $local_ver). Run install.sh to update."
fi
