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

func TestStateMachineSetGet(t *testing.T) {
	sm := NewStateMachine()
	sm.Set("foo", "bar")

	v, ok := sm.Get("foo")
	if !ok {
		t.Fatal("expected key to exist")
	}
	if v != "bar" {
		t.Errorf("expected 'bar', got '%s'", v)
	}
}

func TestStateMachineGetNonExistent(t *testing.T) {
	sm := NewStateMachine()
	_, ok := sm.Get("nonexistent")
	if ok {
		t.Error("expected false for missing key")
	}
}

func TestStateMachineApplySet(t *testing.T) {
	sm := NewStateMachine()
	err := sm.Apply("set foo bar")
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	v, ok := sm.Get("foo")
	if !ok || v != "bar" {
		t.Errorf("expected 'bar', got '%s'", v)
	}
}

func TestStateMachineApplySetMultiWord(t *testing.T) {
	sm := NewStateMachine()
	err := sm.Apply("set message hello world")
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	v, ok := sm.Get("message")
	if !ok || v != "hello world" {
		t.Errorf("expected 'hello world', got '%s'", v)
	}
}

func TestStateMachineApplyGet(t *testing.T) {
	sm := NewStateMachine()
	sm.Set("foo", "bar")
	err := sm.Apply("get foo")
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}
	// get is a no-op for state machine, value should be unchanged
	v, ok := sm.Get("foo")
	if !ok || v != "bar" {
		t.Errorf("expected 'bar', got '%s'", v)
	}
}

func TestStateMachineApplyEmpty(t *testing.T) {
	sm := NewStateMachine()
	err := sm.Apply("")
	if err == nil {
		t.Error("expected error for empty command")
	}
}

func TestStateMachineApplyInvalidSet(t *testing.T) {
	sm := NewStateMachine()
	err := sm.Apply("set")
	if err == nil {
		t.Error("expected error for 'set' without arguments")
	}

	err = sm.Apply("set key")
	if err == nil {
		t.Error("expected error for 'set key' without value")
	}
}

func TestStateMachineApplyUnknown(t *testing.T) {
	sm := NewStateMachine()
	err := sm.Apply("delete foo")
	if err == nil {
		t.Error("expected error for unknown command")
	}
}

func TestStateMachineConcurrent(t *testing.T) {
	sm := NewStateMachine()
	done := make(chan bool)
	for i := 0; i < 100; i++ {
		go func(n int) {
			sm.Set("key", "value")
			sm.Get("key")
			done <- true
		}(i)
	}
	for i := 0; i < 100; i++ {
		<-done
	}
}
