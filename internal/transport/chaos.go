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
	"math/rand"
	"sync/atomic"
)

// dropProbBits holds the current packet loss probability encoded as uint64 bits.
// Use atomic.Load/Store with math.Float64bits/Float64frombits for thread safety.
var dropProbBits atomic.Uint64

// SetDropProbability sets the packet loss probability.
// p must be in [0.0, 1.0]; values outside this range are clamped.
func SetDropProbability(p float64) {
	if p < 0 {
		p = 0
	}
	if p > 1 {
		p = 1
	}
	dropProbBits.Store(math.Float64bits(p))
}

// GetDropProbability returns the current packet loss probability.
func GetDropProbability() float64 {
	return math.Float64frombits(dropProbBits.Load())
}

// ShouldDrop returns true if the current packet should be dropped
// based on the configured loss probability. Exported for use by
// integration tests and the chaos router.
func ShouldDrop() bool {
	prob := math.Float64frombits(dropProbBits.Load())
	return rand.Float64() < prob
}

// shouldDrop is an unexported alias for internal use.
func shouldDrop() bool {
	return ShouldDrop()
}
