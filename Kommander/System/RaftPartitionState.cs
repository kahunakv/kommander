
namespace Kommander.System;

public enum RaftPartitionState { Active, Splitting, Draining, Removed }

public enum RaftRoutingMode { HashRange, Unrouted }
