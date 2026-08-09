# Product Vision

## Thesis

Local developer tools still tend to treat a process as an implementation detail
of the window that launched it. That coupling is fragile for ordinary terminals
and becomes actively limiting for long-running coding agents.

ctxmux makes the Run the durable unit:

> A terminal is a view. An editor is a view. A Run has a life of its own.

The category is a **context-aware Run multiplexer**. It occupies the local
infrastructure layer that tmux occupies for terminal sessions, but its public
model is a Run rather than a terminal layout.

## Who it is for

ctxmux primarily serves:

- client and editor authors who need to embed local process and Agent runtime
  behavior without rebuilding it;
- terminal-first developers who want reliable local Runs that survive client
  churn;
- tool authors composing higher-level workflows such as Crucible, forked
  exploration, or MapReduce without adopting a complete Agent Harness.

## Core object

A `Run` is a durable local execution with a stable identity. Depending on its
declared capabilities, it may own or reference:

- a command, arguments, environment, and working directory;
- a PTY or process I/O channel;
- lifecycle state and ordered output;
- workspace and declared input references;
- artifacts and lineage;
- Integration-defined context needed to resume or fork with higher fidelity.

The minimum promise is not that every hidden process state can be copied. The
promise is that capabilities are explicit and callers can tell exactly which
kind of persistence, resume, or fork they are requesting.

## Product principles

### Runs are universal

Shells, coding agents, servers, tests, benchmarks, and scripts share the same
runtime lifecycle. Agent metadata belongs in an Integration, not in the Run
foundation.

### Context fidelity is pluggable

Every Run has a portable baseline. Richer capture, resume, and fork behavior is
provided by an Integration and exposed as explicit capabilities.

### Clients are disposable

Closing an editor, terminal, or SDK process must not implicitly kill a durable
Run. Clients can reconnect through a stable local boundary.

### Unsupported means unsupported

ctxmux fails closed when a requested capability is unavailable. It does not
silently replace a high-fidelity operation with a weaker one.

### Composition stays above the mux

ctxmux should make parallel search, Context fork, Crucible, and MapReduce easy
to compose. It does not own their scheduling, evaluation, or stopping policy.

### Local-first and embeddable

The runtime works headlessly and does not require an editor, Electron, React,
or a hosted control plane. A CLI and first-party clients use the same public
surface as external clients.

## Success

ctxmux succeeds when a new client can install an SDK, connect to the local
runtime, select or register an Integration, and manage local Runs without
reimplementing PTY transport, lifecycle, reconnect, output, and Integration
differences.

The strongest proof is behavioral: real Runs survive client exit, can be
reattached, and preserve only the context fidelity their capabilities promise.

## Non-goals

- a general Agent planner, scheduler, evaluator, or team runtime;
- copying arbitrary live process memory or undeclared hidden state;
- a distributed or hosted execution platform;
- a plugin marketplace or untrusted plugin sandbox;
- wire compatibility with tmux's private client-server protocol;
- a complete editor inside the core runtime repository.

## Brand

- Name: `ctxmux`, always lowercase in prose and command examples.
- Descriptor: `context-aware Run multiplexer`.
- Thesis: `Terminals are views. Runs are durable.`
- Comparative line: `tmux multiplexes terminals. ctxmux multiplexes runs—with context intact.`
- Voice: direct, technical, opinionated, and concrete; never mystical about
  Agent behavior and never claim orchestration that belongs to clients.
