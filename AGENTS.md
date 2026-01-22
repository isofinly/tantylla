# AGENTS.md

## Responsibility

Developers are ultimately responsible and accountable for all code, whether it is written manually or assisted by an AI. All AI-generated content must be reviewed, edited, and approved by a human developer before being committed.

## Design goals

The design goals focus on building software that is safe, fast, and easy to maintain.

### Control and limits

Predictable control flow and bounded system resources are essential for safe execution.

- Simple and explicit control flow: Favor straightforward control structures over complex logic. Simple control flow makes code easier to understand and reduces the risk of bugs. Avoid recursion if possible to keep execution bounded and predictable, preventing stack overflows and uncontrolled resource use.

- Set fixed limits: Set explicit upper bounds on loops, queues, and other data structures. Fixed limits prevent infinite loops and uncontrolled resource use, following the fail-fast principle. This approach helps catch issues early and keeps the system stable.

### Memory and types

Clear and consistent handling of memory and types is key to writing safe, portable code

- Minimize variable scope: Declare variables in the smallest possible scope. Limiting scope reduces the risk of unintended interactions and misuse. It also makes the code more readable and easier to maintain by keeping variables within their relevant context.

### Error handling

Correct error handling keeps the system robust and reliable in all conditions.

- Use assertions: Use assertions to verify that conditions hold true at specific points in the code. Assertions work as internal checks, increase robustness, and simplify debugging.
  - Assert function arguments and return values: Check that functions receive and return expected values.
  - Validate invariants: Keep critical conditions stable by asserting invariants during execution.
  - Use pair assertions: Check critical data at multiple points to catch inconsistencies early.
  - Fail fast on programmer errors: Detect unexpected conditions immediately, stopping faulty code from continuing.

- Handle all errors: Check and handle every error. Ignoring errors can lead to undefined behavior, security issues, or crashes. Write thorough tests for error-handling code to make sure your application works correctly in all cases.

### Design for performance

Early design decisions have a significant impact on performance. Thoughtful planning helps avoid bottlenecks later.

- Design for performance early: Consider performance during the initial design phase. Early architectural decisions have a big impact on overall performance, and planning ahead ensures you can avoid bottlenecks and improve resource efficiency.

- Napkin math: Use quick, back-of-the-envelope calculations to estimate system performance and resource costs. For example, estimate how long it takes to read 1 GB of data from memory or what the expected storage cost will be for logging 100,000 requests per second. This helps set practical expectations early and identify potential bottlenecks before they occur.

- Batch operations: Amortize expensive operations by processing multiple items together. Batching reduces overhead per item, increases throughput, and is especially useful for I/O-bound operations.

### Efficient resource use

Focus on optimizing the slowest resources, typically in this order:

- Network: Optimize data transfer and reduce latency.
- Disk: Improve I/O operations and manage storage efficiently.
- Memory: Use memory effectively to prevent leaks and overuse.
- CPU: Increase computational efficiency and reduce processing time.

### Developer experience

- Include units or qualifiers in names: Append units or qualifiers to variable names, placing them in descending order of significance (e.g., latency_ms_max instead of max_latency_ms). This clears up meaning, avoids confusion, and ensures related variables, like latency_ms_min, line up logically and group together.

- Document the 'why': Use comments to explain why decisions were made, not just what the code does. Knowing the intent helps others maintain and extend the code properly. Give context for complex algorithms, unusual approaches, or key constraints.

## Project Context

### Project Architecture

- TODO

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
