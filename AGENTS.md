# Agent Instructions & Good Practices

When assisting with this project as an AI agent, you must strictly adhere to the following principles:

- **Incremental Progress & Education:** Never implement entire features at once. Execute only the minimal scope requested. Suggest improvements or next steps, and be prepared to explain your implementation choices clearly to facilitate learning.
- **Constructive Pushback:** Do not be complacent. If a proposed direction, architecture, or solution is suboptimal or incorrect, you must explicitly point it out. Our goal is to build the best software possible, which requires critical and honest feedback.
- **Simplicity First:** Always prefer simple, readable solutions over complex abstractions or premature overengineering. Move little by little, taking small, easily comprehensible steps rather than making massive, sweeping changes in a single go.
- **MVP-First Mindset:** Prioritize getting a working solution in place before optimizing. However, proactively flag scalability concerns, future-proofing opportunities, and architectural considerations when relevant so they can be addressed intentionally rather than discovered painfully later.

## Python Development
- **Strict Typing:** All generated Python code MUST follow strict typing rules. Use type hints (`-> None`, `dict[str, str]`, `str | None`, etc.) for all function signatures and complex variable declarations. Maintain an environment that is clean to static typing checkers and standard Linters (e.g., `ruff`).
- **Type Inference:** Avoid excessive verbosity by relying on type inference whenever possible. Do not explicitly annotate variables if their type is obvious from the assignment (e.g., `topics = ["^shorisql_.*"]` instead of `topics: list[str] = ["^shorisql_.*"]`). Only use explicit type hints for function signatures, complex/ambiguous data structures, or when the type checker cannot infer the type.
