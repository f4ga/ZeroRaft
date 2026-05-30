// Copyright 2026 Ekaterina Godulyan
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import "sync"

// LogEntry represents a single entry in the Raft log.
type LogEntry struct {
	Index   uint64 `json:"index"`
	Term    uint64 `json:"term"`
	Command string `json:"command"`
}

// RaftLog stores log entries with thread-safe access.
// Indexes are 1-based. Index 0 is a sentinel entry with Term 0.
type RaftLog struct {
	mu      sync.RWMutex
	entries []LogEntry
}

// NewRaftLog creates a new log with sentinel entry at index 0.
func NewRaftLog() *RaftLog {
	return &RaftLog{
		entries: []LogEntry{
			{Index: 0, Term: 0, Command: ""}, // sentinel
		},
	}
}

// Append adds an entry to the log. Returns the index of the new entry.
func (l *RaftLog) Append(entry LogEntry) uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, entry)
	return entry.Index
}

// Get returns the log entry at the given index, and whether it exists.
func (l *RaftLog) Get(index uint64) (LogEntry, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if index >= uint64(len(l.entries)) {
		return LogEntry{}, false
	}
	return l.entries[index], true
}

// Last returns the last log entry.
func (l *RaftLog) Last() LogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.entries[len(l.entries)-1]
}

// LastIndex returns the index of the last entry.
func (l *RaftLog) LastIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.entries[len(l.entries)-1].Index
}

// LastTerm returns the term of the last entry.
func (l *RaftLog) LastTerm() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.entries[len(l.entries)-1].Term
}

// Len returns the number of entries (including sentinel).
func (l *RaftLog) Len() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.entries)
}

// TruncateFrom removes all entries starting from the given index.
func (l *RaftLog) TruncateFrom(index uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if index < uint64(len(l.entries)) {
		l.entries = l.entries[:index]
	}
}

// SliceFrom returns all entries starting from the given index.
func (l *RaftLog) SliceFrom(index uint64) []LogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if index >= uint64(len(l.entries)) {
		return nil
	}
	result := make([]LogEntry, len(l.entries)-int(index))
	copy(result, l.entries[index:])
	return result
}

// HasEntry checks if an entry exists at prevLogIndex with prevLogTerm.
func (l *RaftLog) HasEntry(index, term uint64) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if index >= uint64(len(l.entries)) {
		return index == 0 && term == 0 // sentinel match
	}
	return l.entries[index].Term == term
}
