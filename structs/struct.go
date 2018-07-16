package structs

import (
	"net/rpc"
	"time"
)

type Store struct {
	Address   string
	RPCClient *rpc.Client
	IsLeader  bool
}

type WriteRequest struct {
	Key   int
	Value string
}

type ACK struct {
	Acknowledged bool
	Address      string
}

type CandidateInfo struct {
	Term              int
	LogLength         int
	NumberOfCommitted int
}

type LogEntry struct {
	Term        int
	Index       int
	Key         int
	Value       string
	IsCommitted bool
}

type LogEntries struct {
	Current  LogEntry
	Previous LogEntry
}

type StoreInfo struct {
	Address  string
	IsLeader bool
}

type Heartbeat struct {
	Term          int
	LeaderAddress string
	Timestamp     time.Time
}
