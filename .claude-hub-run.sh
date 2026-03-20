#!/bin/bash
claude --output-format stream-json --verbose --dangerously-skip-permissions --model claude-opus-4-6 -p "$(cat /data/clones/6289c5d3-b07/.claude-hub-task.md)" 2>&1 | tee /data/clones/6289c5d3-b07/.claude-hub.jsonl
