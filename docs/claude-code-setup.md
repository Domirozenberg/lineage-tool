# Claude Code Setup

This project is configured for [Claude Code](https://code.claude.com/). Use these files and commands when working with Claude.

## Quick Start

1. **Open project in Claude Code**
   ```bash
   claude /path/to/lineage-tool
   ```

2. **Key files Claude reads**
   - `AGENTS.md` / `CLAUDE.md` - Project conventions and instructions
   - `.cursor/plan.md` - Full implementation plan
   - `docs/PROGRESS.md` - Task tracking

## Slash Commands

| Command | Description |
|---------|-------------|
| `/test` | Run test suite (`python3 -m pytest tests/ -v`) |
| `/progress` | Update docs/PROGRESS.md after completing a task |
| `/setup` | Set up dev environment (venv, pip3 install) |

## Subagents

| Agent | Use when |
|-------|----------|
| `connector-developer` | Building PostgreSQL, Tableau, Snowflake, dbt, or other connectors |
| `code-reviewer` | Reviewing code for quality, security, conventions |

**Example**: "Use the connector-developer agent to implement the PostgreSQL metadata extractor"

## Project Structure

```
lineage-tool/
├── AGENTS.md           # Main agent instructions (industry standard)
├── CLAUDE.md           # Claude Code config (imports AGENTS.md)
├── docs/
│   ├── PROGRESS.md    # Task completion tracking
│   └── architecture.excalidraw  # System architecture diagram
├── .claude/
│   ├── commands/      # Slash commands (/test, /progress, /setup)
│   ├── rules/         # Path-based rules (python, scripts)
│   ├── agents/        # Subagents (connector-developer, code-reviewer)
│   ├── settings.json  # Project permissions
│   └── settings.local.json  # Personal overrides (gitignored)
├── .cursor/
│   ├── plan.md        # Full project plan
│   └── rules/         # Cursor-specific rules
├── scripts/           # Internal scripts only
└── docs/              # Documentation
```

## Conventions (enforced by rules)

- **Python**: Always `pip3` and `python3`
- **Scripts**: Internal scripts in `scripts/` only
- **Progress**: Update docs/PROGRESS.md when task + tests complete

## CLI Usage

```bash
# Start Claude Code in project
claude .

# Run with specific agent
claude --agent connector-developer .

# List configured agents
claude agents
```

## References

- [Claude Code Docs](https://docs.anthropic.com/en/docs/claude-code)
- [Subagents](https://docs.anthropic.com/en/docs/claude-code/sub-agents)
- [.claude Directory](https://dotclaude.com/)
