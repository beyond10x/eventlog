---
format: aep.planning-md/3
id: dependency-blocker:ess-raw-json-rust-mapping
kind: dependency-blocker
status: open
title: ESS Rust target cannot map Json to raw text
relations:
- blocks: story:generate-journal-row-types-from-ess
revision: 1
---
## Blocks

story:generate-journal-row-types-from-ess needs an ESS Rust target option that maps a `Json` value to its raw JSON text: https://github.com/beyond10x/ess/issues/577. Cleared when an ess release ships it and the two journal row types generate.
