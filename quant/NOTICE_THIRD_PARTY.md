# Third-Party Design & License Notice

This repository includes implementation ideas and architecture alignment inspired by publicly documented open-source projects.
No third-party source files are vendored into this repository by default.

## Architecture inspirations

- QuantConnect LEAN
  - Public license observed during comparison: Apache-2.0
  - Borrowed ideas: order event lifecycle, fill/fee/slippage model separation, execution modeling terminology
  - Current repository status: design-aligned reimplementation only

- vn.py / VeighNa
  - Public license observed during comparison: MIT
  - Borrowed ideas: event-driven coordination, operator/workbench decomposition
  - Current repository status: design-aligned reimplementation only

- Microsoft Qlib
  - Public license observed during comparison: MIT
  - Borrowed ideas: provider / dataset / workflow layering
  - Current repository status: design-aligned reimplementation only

- NautilusTrader
  - Public license observed during comparison: LGPL-3.0
  - Borrowed ideas: research/live parity principles, adapter boundary discipline
  - Current repository status: principles only, no direct source reuse

- RQAlpha
  - Public ecosystem licensing/commercial boundary was treated conservatively during planning
  - Borrowed ideas: plugin/mod extensibility direction
  - Current repository status: mechanism-level reimplementation only

## Repository policy

1. If any future code is copied or ported from a third-party repository, the exact source, path, commit, and license must be recorded here.
2. New external code must not be introduced without verifying license compatibility.
3. Design inspiration alone does not imply source-code reuse.
