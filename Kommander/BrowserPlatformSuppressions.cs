#if BROWSER
// Platform-analyzer (CA1416) suppressions for the browser target frameworks, with the reason for
// each. This file compiles only for net8.0-browser and net10.0-browser; the normal targets do not
// analyze for the browser and see none of it.
//
// CA1416 lists every API that the browser does not support. On the single-threaded browser runtime
// such an API fails only when it must block or must reach the operating system. The members below
// call one, but only in a form that does not block, or only on a path that the browser build never
// takes. scripts/run-wasm-smoke.sh runs these paths on the single-threaded runtime under Node.js.
//
// Keep the scope to one member for each entry, so a new call in another member still warns. Before
// you add an entry, prove that the call cannot block or cannot run in the browser build, and write
// the proof in the justification. A call that can block needs an #if, not an entry here.

using System.Diagnostics.CodeAnalysis;

// ── Write-ahead-log write scheduler (manual mode) ──────────────────────────────────────────────
// In the browser build the scheduler is always in manual mode: RaftConfiguration.Validate refuses
// scheduling threads, and the constructor throws without manual mode. The ready queue is then only
// a bookkeeping queue. Every Take with a wait lives in the worker loop, which does not compile here.

[assembly: SuppressMessage("Interoperability", "CA1416",
    Scope = "member",
    Target = "~M:Kommander.WAL.IO.FairWalScheduler.#ctor(Kommander.WAL.IWAL,Microsoft.Extensions.Logging.ILogger{Kommander.IRaft},System.Int32,System.Int32,System.Int32,System.Int32,System.Int32,System.Int32,System.Boolean,Kommander.Time.IMonotonicTickSource,System.Boolean)",
    Justification = "Constructs the BlockingCollection only. Construction does not block.")]

[assembly: SuppressMessage("Interoperability", "CA1416",
    Scope = "member",
    Target = "~M:Kommander.WAL.IO.FairWalScheduler.Enqueue(Kommander.WAL.Data.WALWriteOperation)",
    Justification = "TryAdd does not block. The blocking Add runs only when the bounded ready queue is full, and in manual mode Enqueue drains the queue inline before it returns, so the queue never fills.")]

[assembly: SuppressMessage("Interoperability", "CA1416",
    Scope = "member",
    Target = "~M:Kommander.WAL.IO.FairWalScheduler.PumpOnce~System.Boolean",
    Justification = "TryTake with no timeout does not block. Verified on the single-threaded runtime.")]

[assembly: SuppressMessage("Interoperability", "CA1416",
    Scope = "member",
    Target = "~M:Kommander.WAL.IO.FairWalScheduler.ProcessGroupBatch(System.Collections.Generic.List{System.Int32},System.Collections.Generic.List{System.ValueTuple{System.Int32,System.Collections.Generic.List{Kommander.WAL.Data.WALWriteOperation}}},System.Collections.Generic.List{System.Collections.Generic.List{Kommander.WAL.Data.WALWriteOperation}},System.Collections.Generic.List{System.ValueTuple{System.Int32,System.Collections.Generic.List{Kommander.Data.RaftLog}}})",
    Justification = "TryAdd with no timeout does not block. On a full queue it returns false and the partition is rescheduled by the next Enqueue.")]

[assembly: SuppressMessage("Interoperability", "CA1416",
    Scope = "member",
    Target = "~M:Kommander.WAL.IO.FairWalScheduler.Dispose",
    Justification = "Disposes the BlockingCollection. Dispose does not block.")]

// ── Write-ahead-log read scheduler (inline mode) ───────────────────────────────────────────────
// In the browser build every read runs inline on the caller: RaftManager builds the scheduler with
// inlineExecution when scheduling threads are off, and Start throws without it. A read is finished
// before EnqueueTask returns, so no read is ever in flight when another call looks.

[assembly: SuppressMessage("Interoperability", "CA1416",
    Scope = "member",
    Target = "~M:Kommander.WAL.IO.FairReadScheduler.#ctor(Microsoft.Extensions.Logging.ILogger{Kommander.IRaft},System.Int32,System.Int32,System.Boolean,System.Boolean)",
    Justification = "Constructs the BlockingCollection only. Construction does not block.")]

[assembly: SuppressMessage("Interoperability", "CA1416",
    Scope = "member",
    Target = "~M:Kommander.WAL.IO.FairReadScheduler.TryScheduleLocked(System.Int32,Kommander.WAL.IO.FairReadScheduler.PartitionState)~System.Boolean",
    Justification = "TryAdd on an unbounded queue with no timeout does not block.")]

[assembly: SuppressMessage("Interoperability", "CA1416",
    Scope = "member",
    Target = "~M:Kommander.WAL.IO.FairReadScheduler.Stop",
    Justification = "The ManualResetEventSlim.Wait runs only for a second Stop that overlaps the first. One thread cannot overlap two calls, and a later Stop finds the event already set.")]

[assembly: SuppressMessage("Interoperability", "CA1416",
    Scope = "member",
    Target = "~M:Kommander.WAL.IO.FairReadScheduler.DrainInline",
    Justification = "TryTake with no timeout does not block. Verified on the single-threaded runtime.")]

[assembly: SuppressMessage("Interoperability", "CA1416",
    Scope = "member",
    Target = "~M:Kommander.WAL.IO.FairReadScheduler.DrainRemaining(System.Collections.Generic.List{System.Object})",
    Justification = "TryTake with no timeout does not block. The timed ManualResetEventSlim.Wait runs only while a read is in flight, and inline reads finish before the caller continues.")]

[assembly: SuppressMessage("Interoperability", "CA1416",
    Scope = "member",
    Target = "~M:Kommander.WAL.IO.FairReadScheduler.Dispose",
    Justification = "Disposes the BlockingCollection. Dispose does not block.")]

// ── Certificate-based transport security ───────────────────────────────────────────────────────
// RaftConfiguration.Validate refuses, in the browser build, every option that reaches this code:
// NodeAuthenticationMode.MutualTls, a client certificate or its path, pinned server thumbprints,
// and AllowInsecureCertificateValidation. The browser does TLS itself.

[assembly: SuppressMessage("Interoperability", "CA1416",
    Scope = "member",
    Target = "~M:Kommander.Communication.Rest.RestCommunication.ConfigureClient(Flurl.Http.Configuration.IFlurlClientBuilder,Kommander.RaftTransportSecurityOptions)",
    Justification = "Each certificate branch needs an option that Validate refuses in the browser build. With the default options the handler is left unchanged.")]

[assembly: SuppressMessage("Interoperability", "CA1416",
    Scope = "member",
    Target = "~M:Kommander.RaftClientCertificateValidator.LoadFromRawData(System.Security.Cryptography.X509Certificates.X509Certificate)~System.Security.Cryptography.X509Certificates.X509Certificate2",
    Justification = "Reached only from server-certificate pinning, which Validate refuses in the browser build.")]

[assembly: SuppressMessage("Interoperability", "CA1416",
    Scope = "member",
    Target = "~M:Kommander.RaftTransportSecurityOptions.LoadClientCertificateFromDisk(System.String,System.String)~System.Security.Cryptography.X509Certificates.X509Certificate2",
    Justification = "Reached only with a client certificate path, which Validate refuses in the browser build.")]
#endif
