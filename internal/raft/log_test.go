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

import (
	"testing"
)

func TestLogNewRaftLog(t *testing.T) {
	l := NewRaftLog()
	if l.Len() != 1 {
		t.Errorf("expected 1 sentinel entry, got %d", l.Len())
	}
	if l.LastIndex() != 0 {
		t.Errorf("expected last index 0, got %d", l.LastIndex())
	}
	if l.LastTerm() != 0 {
		t.Errorf("expected last term 0, got %d", l.LastTerm())
	}
}

func TestLogAppendAndGet(t *testing.T) {
	l := NewRaftLog()
	idx := l.Append(LogEntry{Index: 1, Term: 1, Command: "set x 1"})
	if idx != 1 {
		t.Errorf("expected index 1, got %d", idx)
	}

	entry, ok := l.Get(1)
	if !ok {
		t.Fatal("expected entry at index 1")
	}
	if entry.Term != 1 || entry.Command != "set x 1" {
		t.Errorf("wrong entry: %+v", entry)
	}
}

func TestLogGetNonExistent(t *testing.T) {
	l := NewRaftLog()
	_, ok := l.Get(999)
	if ok {
		t.Error("expected false for non-existent index")
	}
}

func TestLogTruncate(t *testing.T) {
	l := NewRaftLog()
	l.Append(LogEntry{Index: 1, Term: 1, Command: "a"})
	l.Append(LogEntry{Index: 2, Term: 1, Command: "b"})
	l.Append(LogEntry{Index: 3, Term: 2, Command: "c"})

	l.TruncateFrom(2)
	if l.Len() != 2 {
		t.Errorf("expected 2 entries after truncate, got %d", l.Len())
	}
	if l.LastIndex() != 1 {
		t.Errorf("expected last index 1, got %d", l.LastIndex())
	}
}

func TestLogSliceFrom(t *testing.T) {
	l := NewRaftLog()
	l.Append(LogEntry{Index: 1, Term: 1, Command: "a"})
	l.Append(LogEntry{Index: 2, Term: 1, Command: "b"})
	l.Append(LogEntry{Index: 3, Term: 2, Command: "c"})

	slice := l.SliceFrom(2)
	if len(slice) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(slice))
	}
	if slice[0].Index != 2 || slice[1].Index != 3 {
		t.Errorf("wrong slice: %+v", slice)
	}
}

func TestLogSliceFromOutOfRange(t *testing.T) {
	l := NewRaftLog()
	slice := l.SliceFrom(100)
	if slice != nil {
		t.Errorf("expected nil for out of range, got %+v", slice)
	}
}

func TestLogHasEntry(t *testing.T) {
	l := NewRaftLog()
	l.Append(LogEntry{Index: 1, Term: 1, Command: "a"})

	if !l.HasEntry(0, 0) {
		t.Error("expected sentinel match at index 0, term 0")
	}
	if !l.HasEntry(1, 1) {
		t.Error("expected match at index 1, term 1")
	}
	if l.HasEntry(1, 2) {
		t.Error("expected mismatch for term 2 at index 1")
	}
	if l.HasEntry(999, 0) {
		t.Error("expected false for non-existent index")
	}
}

func TestLogLast(t *testing.T) {
	l := NewRaftLog()
	last := l.Last()
	if last.Index != 0 || last.Term != 0 {
		t.Errorf("expected sentinel, got %+v", last)
	}

	l.Append(LogEntry{Index: 1, Term: 5, Command: "x"})
	last = l.Last()
	if last.Index != 1 || last.Term != 5 {
		t.Errorf("expected entry 1, got %+v", last)
	}
}
