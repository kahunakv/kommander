# Thread-free embedded mode developer guide

The thread-free embedded mode runs a Kommander node on a host that cannot create threads. The main
target is the default single-threaded .NET WebAssembly runtime (`browser-wasm` with
`WasmEnableThreads` off), in a browser tab or under Node.js.

The mode is a separate build of the library. The compilation symbol `KOMMANDER_THREAD_FREE` turns
it on. A normal build does not define the symbol, so normal behavior does not change.

## Why a separate build

The single-threaded WebAssembly runtime has two hard limits:

1. `new Thread(...).Start()` throws `PlatformNotSupportedException`. A normal Kommander node starts
   threads for the partition executor pool and for the write-ahead-log read and write schedulers.
2. A blocking wait (`.Wait()`, `.Result`, `GetAwaiter().GetResult()`, `SemaphoreSlim.Wait()`) has
   no other thread to release it. It never returns, or it spins until its timeout.

The package also references ASP.NET Core for the gRPC and REST server transports. That framework
has no `browser-wasm` runtime pack, so a browser app that references the normal package fails to
build with `NETSDK1082`.

The experimental multithreaded WebAssembly runtime avoids the first limit. But it crashes in the
runtime's own timer code, it needs COOP/COEP headers, and it does not run under Node.js.

## How to get the thread-free build

The package contains two browser target frameworks, `net8.0-browser` and `net10.0-browser`. Both
define `KOMMANDER_THREAD_FREE`. A project gets them when its own target framework is a `-browser`
one:

```xml
<Project Sdk="Microsoft.NET.Sdk.WebAssembly">
  <PropertyGroup>
    <TargetFramework>net10.0-browser</TargetFramework>
    <RuntimeIdentifier>browser-wasm</RuntimeIdentifier>
    <WasmEnableThreads>false</WasmEnableThreads>
    <JsonSerializerIsReflectionEnabledByDefault>true</JsonSerializerIsReflectionEnabledByDefault>
  </PropertyGroup>
</Project>
```

Two settings in this example are necessary:

- **`net10.0-browser`, not `net10.0`.** A WebAssembly app that targets plain `net10.0` gets the
  normal assembly. The build then fails with `NETSDK1082`. If it builds, the node fails on the first
  `Thread.Start`.
- **`JsonSerializerIsReflectionEnabledByDefault`.** A WebAssembly app turns off reflection-based
  `System.Text.Json` by default. Kommander serializes the partition map and other system state with
  reflection. Without this property, the system coordinator fails on the first partition-map
  proposal, and `JoinCluster` never finishes.

## What the build changes

| Area | Normal build | Thread-free build |
| --- | --- | --- |
| Partition executors | Shared pool of worker threads, or a thread for each partition | Shared pool in manual mode, driven by the host pump. The thread-for-each-partition mode is not available. |
| Write-ahead-log writes | Worker threads | Written inline when enqueued |
| Write-ahead-log reads | Worker threads | Run inline on the caller |
| Outbound transport | A task loop for each peer | Flushed by the host pump |
| gRPC and REST server transports | Included | Not included |
| gRPC client transport | Included | Not included. It needs `SocketsHttpHandler`, which the browser does not have. The REST client and the in-memory transport stay. |
| `MulticastDiscovery` | Included | Not included. It needs UDP sockets. |
| Certificate-based transport security | Supported | Refused by `Validate`: `NodeAuthenticationMode.MutualTls`, a client certificate, pinned server thumbprints, and `AllowInsecureCertificateValidation`. The browser does TLS itself. Use `Disabled` or `SharedSecret`. |
| ASP.NET Core reference | Yes | No |
| RocksDB and SQLite write-ahead logs | Included | Not included, with their packages (`RocksDB`, `Microsoft.Data.Sqlite`, `SQLitePCLRaw.lib.e_sqlite3`) |

## The host pump

In the thread-free build, `RaftConfiguration.EnableHostPumpedScheduling` exists and defaults to
`true`. `EnableInternalSchedulingThreads` defaults to `false`. A host that sets neither option gets
a node that runs.

The host pump replaces the worker threads. It runs as an async loop on the ambient scheduler. On
single-threaded WebAssembly, that scheduler is the browser event loop. On each pass the pump:

1. Starts a drain for each ready partition executor, up to 64 at one time. It does not await a
   drain, because a drain can wait for work that a later pass does.
2. Writes pending write-ahead-log batches.
3. Flushes the outbound transport.

When a pass moves no work, the pump waits for the first of these: an executor is scheduled, an
in-flight drain finishes, or an idle timeout expires. The timeout starts at 1 ms and doubles to a
maximum of 20 ms. An idle node wakes a few times a second. A busy node does not wait.

Every wait in the pump is an await. The pump never blocks the thread.

## Configuration rules

`RaftConfiguration.Validate` refuses these configurations in the thread-free build:

- `EnableInternalSchedulingThreads = true`. The build has no code to start those threads.
- `EnableSharedExecutorPool = false`. A partition executor on its own thread has nothing to pump.

Set `EnableHostPumpedScheduling = false` only when something else drives the node, as a
deterministic simulation does. With the pump off and no driver, nothing runs.

## Storage and discovery

Use the in-memory write-ahead log (`InMemoryWAL`). The browser targets do not contain `RocksDbWAL`
or `SqliteWAL`. These backends need native libraries and file-system access that the browser does
not have, so the browser targets leave them out with their packages. This makes the app download
smaller.

Use `StaticDiscovery`. The browser targets do not contain `MulticastDiscovery`, because it needs
UDP sockets.

Do not use `Microsoft.Extensions.Logging.Console` as the logger. Its processor writes from a
dedicated thread. Use a logger that writes synchronously.

## Teardown

Call `LeaveCluster(dispose: true)`. It awaits the partition drains before it disposes the node.

A direct call to `Dispose` is synchronous, and the thread-free build cannot block in it. If a drain
does not finish at once, `Dispose` logs a warning and continues without that drain.

## Testing the thread-free build

Two checks cover the build. The CI workflow runs both.

1. **Tests on normal .NET.** `-p:KommanderThreadFree=true` defines the symbol for the normal target
   frameworks. The flagged build writes to its own `bin/threadfree/` and `obj/threadfree/`
   folders, so it does not replace the normal build output.

   ```sh
   dotnet test Kommander.Tests/Kommander.Tests.csproj -p:KommanderThreadFree=true \
     --filter "FullyQualifiedName~TestHostPumpedScheduling"
   ```

   These tests prove that the pump alone makes a node elect and commit. They cannot prove that no
   code blocks a thread. Library code resumes with `ConfigureAwait(false)`, so on normal .NET a
   blocked continuation moves to another thread and the test still passes.

2. **Smoke check on single-threaded WebAssembly.** `scripts/run-wasm-smoke.sh` builds
   `Kommander.WasmSmoke` for `browser-wasm` with `WasmEnableThreads` off and runs it under Node.js.
   The app starts a single node, waits for the election, commits five proposals, and leaves. A
   thread start, a blocking wait, or a stall makes it fail. It needs the .NET 10 SDK and Node.js,
   but not the `wasm-tools` workload.

   ```sh
   scripts/run-wasm-smoke.sh
   ```

## Rules for changes to the library

- Put thread-free code under `#if KOMMANDER_THREAD_FREE`. Keep each region small, and write a
  comment that tells why the region is gated.
- Code that names a type that only the browser targets remove (a server transport, `RocksDbWAL`,
  or `SqliteWAL`) goes under `#if !BROWSER`. The SDK defines `BROWSER` for `-browser` target
  frameworks. Do not use `KOMMANDER_THREAD_FREE` for it: the `-p:KommanderThreadFree=true` build
  keeps these types, because the normal test project uses them.
- Do not change the normal path to share code with the thread-free path. When an async form would
  change the normal path, put it under `#if KOMMANDER_THREAD_FREE` and keep the current code in
  `#else`.
- A new `Thread` start site or a new blocking wait on the embedded path breaks the smoke check.
  The browser targets build with no warnings, so a new `CA1416` warning there is a new call to an
  API that the browser does not support. Remove the call from the browser build, or put it under
  `#if`. Add an entry to `Kommander/BrowserPlatformSuppressions.cs` only when you can prove that
  the call cannot block or cannot run in the browser build, and write that proof in the entry's
  justification.
