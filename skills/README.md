# AI Agent Skills for Memgraph

Portable AI agent skills following the [Agent Skills Standard](https://agentskills.io/specification).

## Available Skills

| Skill | Description |
|-------|-------------|
| `memgraph-storage-reviewer` | Expert code reviewer for storage layer (MVCC, WAL, DDL, indices) |
| `memgraph-generate-docker-images` | Build MAGE Docker images locally for testing |
| `memgraph-mage-e2e-testing` | Build a MAGE Docker image and run e2e tests for query modules |

## Setup by Tool

### OpenAI Codex

Symlink to the standard location:
```bash
ln -s ../skills .agents/skills
```

### Claude Code

Claude Code has two integration points:

**Skills** (invoke with `/skill-name`, Claude auto-loads when relevant):
```bash
mkdir -p .claude/skills
ln -s ../../skills/memgraph-storage-reviewer .claude/skills/
```

**Agents** (spawned as subagents via Task tool):
```bash
ln -s ../skills/memgraph-storage-reviewer/SKILL.md \
      .claude/agents/memgraph-storage-reviewer.md
```

Set up both for full functionality.

### Cursor

Reference in `.cursorrules`:
```
When reviewing code in src/storage/v2/, follow:
skills/memgraph-storage-reviewer/SKILL.md
```

### GitHub Copilot

Add to `.github/copilot-instructions.md`:
```
For storage layer reviews, follow: skills/memgraph-storage-reviewer/SKILL.md
```

### Windsurf / Codeium

Add to `.windsurfrules`:
```
@file:skills/memgraph-storage-reviewer/SKILL.md
```

### Aider

```bash
aider --read skills/memgraph-storage-reviewer/SKILL.md
```

## Creating New Skills

```
skills/my-skill/
├── SKILL.md           # Required
└── references/        # Optional: additional docs
```

Frontmatter:
```yaml
---
name: my-skill
description: When to activate this skill
---
```

See [agentskills.io/specification](https://agentskills.io/specification) for the full spec.
