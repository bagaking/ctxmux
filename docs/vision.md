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

ctxmux is independently useful at this layer. The daemon and CLI provide the
standalone Run lifecycle; SDK consumers, editors, and Agent products are
optional clients of the same public boundary.

## Who it is for

ctxmux primarily serves:

- client and editor authors who need to embed local process runtime foundations
  without rebuilding PTY and process ownership;
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
runtime lifecycle. Provider sessions, permissions, messages, and Agent status
belong to an embedding client's Provider or Integration layer, not in the Run
foundation.

### Context fidelity is pluggable

Every Run has a portable baseline. Richer capture, resume, and fork behavior is
supplied as an explicit, fully materialized plan by a host-side Integration or
Provider and exposed as an explicit capability. ctxmux executes and records the
declared Run operation; it does not infer provider semantics from terminal
output.

### Clients are disposable

Closing an editor, terminal, or SDK process must not implicitly kill a durable
Run. Clients can reconnect through a stable local boundary.

Run operations expose only facts owned by the runtime. For input, that can
include whether one exact byte range crossed the PTY write boundary and whether
a lost response can be recovered without writing the bytes twice. It does not
include whether a terminal application interpreted those bytes as a submitted
message.

### Unsupported means unsupported

ctxmux fails closed when a requested capability is unavailable. It does not
silently replace a high-fidelity operation with a weaker one.

### Composition stays above the mux

ctxmux should make parallel search, Context fork, Crucible, and MapReduce easy
to compose. It does not own their scheduling, evaluation, or stopping policy.
Message delivery, semantic acknowledgement, reply correlation, and task state
likewise remain embedding-client responsibilities rather than daemon Run
semantics.

### Local-first and embeddable

The runtime works headlessly and does not require an editor, Electron, React,
or a hosted control plane. A CLI and first-party clients use the same public
surface as external clients.

### Standalone is a product invariant

ctxmux is a complete local Runtime product, not a helper library whose basic
lifecycle depends on a higher-level Agent client. A clean installation must be
able to activate its daemon, start an arbitrary command as a Run, detach and
reattach, send input and resize, replay ordered output, wait for terminal state,
stop the owned process scope, and inspect runtime and Run identity through
ctxmux's own CLI or SDK.

Provider-neutral launch recipes, the shell Integration, portable Level A fork,
and execution of a caller-materialized Level B `RunSpec` remain ctxmux
capabilities. Provider session extraction, provider-native resume, semantic
replay interpretation, working-state and permission policy, Agent-to-Agent
messaging, scheduling, and evaluation belong to higher clients. Missing Level B
provenance is an explicit unsupported result; it never silently becomes Level A.

## Success

ctxmux succeeds first when its published daemon and CLI pass the complete
standalone lifecycle in a clean environment with no higher-level client or
provider CLI installed. It additionally succeeds as an embedding surface when
a new client can install an SDK, connect to the local runtime, and manage local
Runs without reimplementing activation, PTY transport, lifecycle, reconnect,
output, identity, or the same-connection Runtime identity and capability
dispatch fences.

The strongest proof is behavioral: real Runs survive client exit, can be
reattached, and preserve only the context fidelity their capabilities promise.

## Non-goals

- a general Agent planner, scheduler, evaluator, or team runtime;
- ownership of provider sessions, provider-native resume, semantic transcript
  reconstruction, working-state or permission policy, or Agent-to-Agent
  messaging;
- copying arbitrary live process memory or undeclared hidden state;
- a hosted control plane, Relay, account/environment federation, or remote
  scheduling platform; a future owner-host Runtime transport is not excluded;
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
