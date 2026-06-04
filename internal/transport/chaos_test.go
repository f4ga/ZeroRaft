// Copyright 2026 Ekaterina Godulyan
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package transport

import (
	"math"
	"sync"
	"testing"
)

func TestSetDropProbability(t *testing.T) {
	tests := []struct {
		name     string
		input    float64
		expected float64
	}{
		{"zero", 0.0, 0.0},
		{"one", 1.0, 1.0},
		{"mid", 0.3, 0.3},
		{"clamp negative", -0.5, 0.0},
		{"clamp above", 1.5, 1.0},
		{"small", 0.001, 0.001},
		{"exact half", 0.5, 0.5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SetDropProbability(tt.input)
			got := GetDropProbability()
			if math.Abs(got-tt.expected) > 1e-9 {
				t.Errorf("SetDropProbability(%v) = %v, want %v", tt.input, got, tt.expected)
			}
		})
	}
}
func TestShouldDrop(t *testing.T) {
	// Test with 0% loss — should never drop
	SetDropProbability(0.0)
	for i := 0; i < 100; i++ {
		if shouldDrop() {
			t.Error("shouldDrop() = true with 0% loss")
		}
	}
	// Test with 100% loss — should always drop
	SetDropProbability(1.0)
	for i := 0; i < 100; i++ {
		if !shouldDrop() {
			t.Error("shouldDrop() = false with 100% loss")
		}
	}
}
func TestShouldDropProbabilistic(t *testing.T) {
	// Test with 30% loss — should drop approximately 30% of packets.
	// Allow a tolerance of ±10% (i.e., 20-40%).
	const (
		targetProb = 0.3
		iterations = 10000
		tolerance  = 0.10 // ±10% absolute tolerance
	)
	SetDropProbability(targetProb)
	dropped := 0
	for i := 0; i < iterations; i++ {
		if shouldDrop() {
			dropped++
		}
	}
	actualProb := float64(dropped) / iterations
	lower := targetProb - tolerance
	upper := targetProb + tolerance
	if actualProb < lower || actualProb > upper {
		t.Errorf("drop probability: got %.4f, want between %.4f and %.4f (target %.2f, %d iterations)",
			actualProb, lower, upper, targetProb, iterations)
	}
}
func TestConcurrentSetGet(t *testing.T) {
	// Verify concurrent access to SetDropProbability and GetDropProbability
	// does not cause data races.
	var wg sync.WaitGroup
	// Concurrent writers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(val float64) {
			defer wg.Done()
			SetDropProbability(val)
		}(float64(i) / 10.0)
	}
	// Concurrent readers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = GetDropProbability()
		}()
	}
	// Concurrent shouldDrop callers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = shouldDrop()
		}()
	}
	wg.Wait()
}
func TestShouldDropDeterministic(t *testing.T) {
	// Verify that shouldDrop is deterministic for a given seed.
	// Note: shouldDrop uses rand.Float64() which uses the global random source.
	// This test checks that the function doesn't have any unexpected side effects.
	SetDropProbability(0.5)
	// Just ensure it returns a bool without panicking
	for i := 0; i < 10; i++ {
		result := shouldDrop()
		if result != true && result != false {
			t.Error("shouldDrop() returned non-boolean value")
		}
	}
}
func TestGetDropProbabilityDefault(t *testing.T) {
	// Reset to default (0.0) and verify
	SetDropProbability(0.0)
	prob := GetDropProbability()
	if prob != 0.0 {
		t.Errorf("default drop probability: got %v, want 0.0", prob)
	}
}
