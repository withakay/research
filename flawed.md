# flawed.md — known issues & intentional shortcuts

This file documents known limitations, shortcuts, and intentional "flaws" in
the projects inside this repository.  Before raising or auto-fixing an apparent
bug, check here to see if it is already acknowledged.

For full agent guidance, see [agents.md](agents.md).

## General

* **No root-level test suite.** Each project (if it has tests at all) runs its
  own tests independently.  There is no `make test` or similar at the repo
  root.
* **Inconsistent code style.** Projects were written at different times, by
  different people, in different moods.  Style inconsistency is expected and
  not worth fixing unless it actively causes problems.
* **Abandoned experiments.** Some folders may contain incomplete or broken
  code that was never finished.  This is intentional — the goal is to save the
  idea, not to ship a product.

## Per-project issues

<!-- Add per-project notes below as they are discovered.
     Format:
     ### <folder-name>
     * Brief description of the known issue or shortcut.
-->
