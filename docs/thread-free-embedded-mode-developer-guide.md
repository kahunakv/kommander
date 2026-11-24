# Thread-free embedded mode developer guide

The thread-free embedded mode is for a Kommander node on a host that cannot create threads. The main
target is the default single-threaded .NET WebAssembly runtime (`browser-wasm` with
`WasmEnableThreads` off), in a browser tab or under Node.js.

The mode is a separate build of the library, turned on by the compilation symbol
`KOMMANDER_THREAD_FREE`. A normal build does not define the symbol and its behavior does not change.

## Runtime limits

The single-threaded WebAssembly runtime has two hard limits.

1. `new Thread(...).Start()` throws `PlatformNotSupportedException`. A normal Kommander node starts
   threads for the partition executor pool and for the write-ahead-log read and write schedulers.
2. A blocking wait (`.Wait()`, `.Result`, `GetAwaiter().GetResult()`, `SemaphoreSlim.Wait()`)
   cannot be released by another thread. It hangs, or it spins until its timeout.

The package also references ASP.NET Core for the gRPC and REST server transports. A `browser-wasm`
runtime pack for that framework does not exist, and a browser app that references the normal
package does not build (`NETSDK1082`).

The experimental multithreaded WebAssembly runtime avoids the first limit. However, it crashes in
the runtime's timer code. It also needs COOP/COEP headers and does not work under Node.js.

## How to get the thread-free build

The package contains three browser target frameworks, `net8.0-browser`, `net10.0-browser` and
`net11.0-browser`. All three define `KOMMANDER_THREAD_FREE`. A project gets them when its
target framework is a `-browser` one.

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

Two settings in this example are necessary.

- **Target `net10.0-browser`.** A WebAssembly app that targets `net10.0` gets the normal
  assembly. The build then stops on `NETSDK1082`. If it builds, the node fails on the first
  `Thread.Start`.
- **`JsonSerializerIsReflectionEnabledByDefault`.** A WebAssembly app turns off reflection-based
  `System.Text.Json` by default. Kommander serializes the partition map and other system state with
  reflection. If this property is missing, the system coordinator fails on the first partition-map
  proposal and `JoinCluster` does not finish.

## What the build changes

| Area | Normal build | Thread-free build |
| --- | --- | --- |
| Partition executors | Shared pool of worker threads, or a thread for each partition | Shared pool in manual mode, driven by the host pump. The thread-for-each-partition mode is not available. |
| Write-ahead-log writes | Worker threads | Written inline when enqueued |
| Write-ahead-log reads | Worker threads | Run inline on the caller |
| Outbound transport | A task loop for each peer | Flushed by the host pump |
| gRPC and REST server transports | Included | Not included |
| gRPC client transport | Included | Not included. It needs `SocketsHttpHandler`, which the browser does not have. The REST client and the in-memory transport are included. |
| `MulticastDiscovery` | Included | Not included. It needs UDP sockets. |
| Certificate-based transport security | Supported | `Validate` throws for `NodeAuthenticationMode.MutualTls`, a client certificate, pinned server thumbprints, and `AllowInsecureCertificateValidation`. The browser does TLS itself. Use `Disabled` or `SharedSecret`. |
| ASP.NET Core reference | Yes | No |
| RocksDB and SQLite write-ahead logs | Included | Not included, with their packages (`RocksDB`, `Microsoft.Data.Sqlite`, `SQLitePCLRaw.lib.e_sqlite3`) |

## The host pump

In the thread-free build, `RaftConfiguration.EnableHostPumpedScheduling` exists and defaults to
`true`. `EnableInternalSchedulingThreads` defaults to `false`. A host that sets neither option gets
a node that works.

The host pump replaces the worker threads. It is an async loop on the ambient scheduler, which is
the browser event loop on single-threaded WebAssembly. Each pass has three steps.

1. Start a drain for each ready partition executor, up to 64 at one time. The pump does not await a
   drain. A drain can wait for work that a later pass does.
2. Write pending write-ahead-log batches.
3. Flush the outbound transport.

After a pass that moves no work, the pump waits until an executor is scheduled or an in-flight
drain finishes, with an idle timeout. The timeout starts at 1 ms and doubles to a maximum of 20 ms.
An idle node wakes a few times a second. A busy node does not wait.

All waits in the pump are awaits, and the pump does not block the thread.

## Configuration rules

`RaftConfiguration.Validate` throws for two configurations in the thread-free build.

- `EnableInternalSchedulingThreads = true`. The build has no code to start those threads.
- `EnableSharedExecutorPool = false`. The host pump drives the shared pool only.

Set `EnableHostPumpedScheduling = false` only if another driver, such as a deterministic simulation,
drives the node. Otherwise the node does no work.

## Storage and discovery

Use the in-memory write-ahead log (`InMemoryWAL`). The browser targets do not contain `RocksDbWAL`
or `SqliteWAL`. These backends need native libraries and file-system access that the browser does
not have.

Use `StaticDiscovery`. `MulticastDiscovery` needs UDP sockets, which the browser does not have.

Do not use `Microsoft.Extensions.Logging.Console` as the logger. Its processor writes from a
dedicated thread. Use a logger that writes synchronously.

## Teardown

Call `LeaveCluster(dispose: true)`. It awaits the partition drains before it disposes the node.

A direct call to `Dispose` is synchronous, and the thread-free build cannot block in it. If a drain
does not finish at once, `Dispose` logs a warning and does not wait for it.

## Testing the thread-free build

The CI workflow has two checks for the build.

1. **Tests on normal .NET.** `-p:KommanderThreadFree=true` defines the symbol for the normal target
   frameworks. The flagged build writes to `bin/threadfree/` and `obj/threadfree/` and does
   not replace the normal build output.

   ```sh
   dotnet test Kommander.Tests/Kommander.Tests.csproj -p:KommanderThreadFree=true \
     --filter "FullyQualifiedName~TestHostPumpedScheduling"
   ```

   These tests prove that the pump alone makes a node elect and commit. They cannot prove that no
   code blocks a thread. Library code resumes with `ConfigureAwait(false)`. On normal .NET a blocked
   continuation moves to another thread and the test passes.

2. **Smoke check on single-threaded WebAssembly.** `scripts/run-wasm-smoke.sh` publishes
   `Kommander.WasmSmoke` for `browser-wasm` with `WasmEnableThreads` off and starts it under
   Node.js. The app starts a single node and commits five proposals after the election. It fails on
   a thread start, a blocking wait, or a stall. It needs the .NET 10 SDK and Node.js, but not the
   `wasm-tools` workload. To check the `net11.0-browser` assembly, set
   `SMOKE_FRAMEWORK=net11.0-browser`; this needs the .NET 11 SDK.

   ```sh
   scripts/run-wasm-smoke.sh
   ```

## Rules for changes to the library

- Put thread-free code under `#if KOMMANDER_THREAD_FREE`, in small regions. Write a comment that
  tells why each region is gated.
- Put code that names a type removed only by the browser targets (a server transport, `RocksDbWAL`,
  or `SqliteWAL`) under `#if !BROWSER`. The SDK defines `BROWSER` for `-browser` target frameworks.
  Do not use `KOMMANDER_THREAD_FREE` for it. The `-p:KommanderThreadFree=true` build contains these
  types for the normal test project.
- Do not change the normal path to share code with the thread-free path. When an async form would
  change the normal path, put it under `#if KOMMANDER_THREAD_FREE` and keep the current code in
  `#else`.
- A new `Thread` start site or a new blocking wait on the embedded path breaks the smoke check. The
  browser targets have a warning-free build. A new `CA1416` warning there is a new call to an API
  that the browser does not support. Remove the call from the browser build, or put it under `#if`.
  An entry in `Kommander/BrowserPlatformSuppressions.cs` needs a proof, in its justification, that
  the call cannot block or cannot run in the browser build.
