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

// ── Write-ahead-log read scheduler (inline mode) ───────────────────────────────────────────────
// In the browser build every read runs inline on the caller: RaftManager builds the scheduler with
// inlineExecution when scheduling threads are off, and Start throws without it. A read is finished
// before EnqueueTask returns, so no read is ever in flight when another call looks. The worker loop
// and its park are compiled out of the thread-free build; the ready queue is a ConcurrentQueue and the
// idle-worker stack is always empty, so admission and the inline drain call nothing that blocks.

[assembly: SuppressMessage("Interoperability", "CA1416",
    Scope = "member",
    Target = "~M:Kommander.WAL.IO.FairReadScheduler.Stop",
    Justification = "The ManualResetEventSlim.Wait runs only for a second Stop that overlaps the first. One thread cannot overlap two calls, and a later Stop finds the event already set.")]

[assembly: SuppressMessage("Interoperability", "CA1416",
    Scope = "member",
    Target = "~M:Kommander.WAL.IO.FairReadScheduler.DrainRemaining(System.Collections.Generic.List{System.Object})",
    Justification = "TryDequeue does not block. The timed ManualResetEventSlim.Wait runs only while a read is in flight, and inline reads finish before the caller continues.")]

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
