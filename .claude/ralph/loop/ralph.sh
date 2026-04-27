#!/bin/bash
# Ralph Wiggum - Long-running AI agent loop
# Usage: ./ralph.sh [--tool amp|claude] [max_iterations]
# max_iterations=0 means run until complete or a hard blocker is reported.

set -e

# Parse arguments
TOOL="amp"  # Default to amp for backwards compatibility
MAX_ITERATIONS=0

while [[ $# -gt 0 ]]; do
  case $1 in
    --tool)
      TOOL="$2"
      shift 2
      ;;
    --tool=*)
      TOOL="${1#*=}"
      shift
      ;;
    *)
      # Assume it's max_iterations if it's a number
      if [[ "$1" =~ ^[0-9]+$ ]]; then
        MAX_ITERATIONS="$1"
      fi
      shift
      ;;
  esac
done

# Validate tool choice
if [[ "$TOOL" != "amp" && "$TOOL" != "claude" ]]; then
  echo "Error: Invalid tool '$TOOL'. Must be 'amp' or 'claude'."
  exit 1
fi
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
PRD_FILE="$SCRIPT_DIR/prd.json"
PROGRESS_FILE="$SCRIPT_DIR/progress.txt"
ARCHIVE_DIR="$SCRIPT_DIR/archive"
LAST_BRANCH_FILE="$SCRIPT_DIR/.last-branch"

get_remaining_count() {
  jq '[.userStories[] | select(.passes != true)] | length' "$PRD_FILE"
}

get_next_story_id() {
  jq -r '.userStories | map(select(.passes != true)) | sort_by(.priority, .id) | .[0].id // empty' "$PRD_FILE"
}

# Archive previous run if branch changed
if [ -f "$PRD_FILE" ] && [ -f "$LAST_BRANCH_FILE" ]; then
  CURRENT_BRANCH=$(jq -r '.branchName // empty' "$PRD_FILE" 2>/dev/null || echo "")
  LAST_BRANCH=$(cat "$LAST_BRANCH_FILE" 2>/dev/null || echo "")
  
  if [ -n "$CURRENT_BRANCH" ] && [ -n "$LAST_BRANCH" ] && [ "$CURRENT_BRANCH" != "$LAST_BRANCH" ]; then
    # Archive the previous run
    DATE=$(date +%Y-%m-%d)
    # Strip "ralph/" prefix from branch name for folder
    FOLDER_NAME=$(echo "$LAST_BRANCH" | sed 's|^ralph/||')
    ARCHIVE_FOLDER="$ARCHIVE_DIR/$DATE-$FOLDER_NAME"
    
    echo "Archiving previous run: $LAST_BRANCH"
    mkdir -p "$ARCHIVE_FOLDER"
    [ -f "$PRD_FILE" ] && cp "$PRD_FILE" "$ARCHIVE_FOLDER/"
    [ -f "$PROGRESS_FILE" ] && cp "$PROGRESS_FILE" "$ARCHIVE_FOLDER/"
    echo "   Archived to: $ARCHIVE_FOLDER"
    
    # Reset progress file for new run
    echo "# Ralph Progress Log" > "$PROGRESS_FILE"
    echo "Started: $(date)" >> "$PROGRESS_FILE"
    echo "---" >> "$PROGRESS_FILE"
  fi
fi

# Track current branch
if [ -f "$PRD_FILE" ]; then
  CURRENT_BRANCH=$(jq -r '.branchName // empty' "$PRD_FILE" 2>/dev/null || echo "")
  if [ -n "$CURRENT_BRANCH" ]; then
    echo "$CURRENT_BRANCH" > "$LAST_BRANCH_FILE"
  fi
fi

# Initialize progress file if it doesn't exist
if [ ! -f "$PROGRESS_FILE" ]; then
  echo "# Ralph Progress Log" > "$PROGRESS_FILE"
  echo "Started: $(date)" >> "$PROGRESS_FILE"
  echo "---" >> "$PROGRESS_FILE"
fi

echo "Starting Ralph - Tool: $TOOL - Max iterations: $MAX_ITERATIONS"
if [[ "$MAX_ITERATIONS" -eq 0 ]]; then
  echo "Mode: run until all stories are complete or a hard blocker is reported."
fi

i=1
while true; do
  if [[ "$MAX_ITERATIONS" -gt 0 && "$i" -gt "$MAX_ITERATIONS" ]]; then
    echo ""
    echo "Ralph reached max iterations ($MAX_ITERATIONS) without completing all tasks."
    echo "Check $PROGRESS_FILE for status."
    exit 1
  fi

  REMAINING_BEFORE=$(get_remaining_count)
  if [[ "$REMAINING_BEFORE" -eq 0 ]]; then
    echo ""
    echo "Ralph completed all tasks before starting iteration $i."
    exit 0
  fi

  NEXT_STORY=$(get_next_story_id)
  echo ""
  echo "==============================================================="
  if [[ "$MAX_ITERATIONS" -eq 0 ]]; then
    echo "  Ralph Iteration $i (until complete) ($TOOL) - next: $NEXT_STORY"
  else
    echo "  Ralph Iteration $i of $MAX_ITERATIONS ($TOOL) - next: $NEXT_STORY"
  fi
  echo "==============================================================="

  # Run the selected tool with the ralph prompt
  if [[ "$TOOL" == "amp" ]]; then
    OUTPUT=$(cd "$REPO_ROOT" && cat "$SCRIPT_DIR/prompt.md" | amp --dangerously-allow-all 2>&1 | tee /dev/stderr) || true
  else
    # Claude Code: use --dangerously-skip-permissions for autonomous operation, --print for output
    OUTPUT=$(cd "$REPO_ROOT" && claude --dangerously-skip-permissions --print < "$SCRIPT_DIR/CLAUDE.md" 2>&1 | tee /dev/stderr) || true
  fi
  
  # Check for completion signal
  if echo "$OUTPUT" | grep -q "<promise>COMPLETE</promise>"; then
    echo ""
    echo "Ralph completed all tasks!"
    if [[ "$MAX_ITERATIONS" -eq 0 ]]; then
      echo "Completed at iteration $i"
    else
      echo "Completed at iteration $i of $MAX_ITERATIONS"
    fi
    exit 0
  fi

  if echo "$OUTPUT" | grep -q "<promise>BLOCKED</promise>"; then
    echo ""
    echo "Ralph reported a hard blocker on $NEXT_STORY."
    echo "Check $PROGRESS_FILE for the exact evidence."
    exit 2
  fi

  REMAINING_AFTER=$(get_remaining_count)
  if [[ "$REMAINING_AFTER" -eq 0 ]]; then
    echo ""
    echo "Ralph completed all tasks!"
    if [[ "$MAX_ITERATIONS" -eq 0 ]]; then
      echo "Completed at iteration $i"
    else
      echo "Completed at iteration $i of $MAX_ITERATIONS"
    fi
    exit 0
  fi

  if [[ "$REMAINING_AFTER" -lt "$REMAINING_BEFORE" ]]; then
    echo "Iteration $i complete. Remaining stories: $REMAINING_AFTER"
  else
    echo "Iteration $i complete with no newly passed story. Remaining stories: $REMAINING_AFTER"
  fi
  i=$((i + 1))
  sleep 2
done
