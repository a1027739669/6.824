package raft

// election
const (
	// magic number
	voted_nil int = -1
)

// appendEntries
const (
	magic_index int = 0
	magic_term  int = -1
)

// ticker
const (
	gap_time            int = 3
	election_base_time  int = 200
	election_range_time int = 100
	heartbeat_time      int = 100
)
