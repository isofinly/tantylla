# AGENTS.md

## Responsibility

Developers are ultimately responsible and accountable for all code, whether it is written manually or assisted by an AI. All AI-generated content must be reviewed, edited, and approved by a human developer before being committed.

## Project Context

### Project Architecture

```
.
├── rust/ # All rust code. There's an AGENTS.md file specific to Rust components. Make sure to provide correct path. There's no rust code in the root of the project.
├── cql-queries/ # CQL queries for ScyllaDB
├── compose.yaml # Configuration to run dev scylladb
├── flake.nix # Configuration of all environment for this project
├── README.md # Description of the project
└── AGENTS.md # This file
```

### Commands

- TODO

### Coding Conventions

- TODO

## How to Work with this Project

### For New Features

1.  Use AI to generate a boilerplate for the new feature in a separate branch.
2.  Follow the architectural and style guidelines defined above to integrate the AI-generated code.
3.  Write tests for the new feature using the standard commands.
4.  Ensure all tests pass and that the code adheres to the project's conventions before submitting a pull request.

### For Bug Fixes

1.  Use AI to help analyze the bug report and suggest potential fixes.
2.  **Manually** implement the fix, as AI output is not guaranteed to be correct.
3.  Add or update a test that reproduces the bug and confirms the fix.
