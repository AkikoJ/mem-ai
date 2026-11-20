# Domain Docs

Engineering skills should read the following domain documentation before exploring the codebase.

## Before exploring

- Read `GLOSSARY.md` at the repo root
- Read ADRs under `docs/adr/` that touch the area being changed
- Use the vocabulary defined in `GLOSSARY.md`
- Surface any contradiction with an existing ADR instead of silently overriding it

## File structure

This is a single-context repository.

```text
/
├── GLOSSARY.md
├── docs/
│   ├── adr/
│   └── agents/
└── src/
```

If a domain concept is missing from the glossary, treat that as a signal to clarify the vocabulary before introducing it into issues, design documents or test names.
