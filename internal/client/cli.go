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

package client

import "errors"

// CLI представляет интерфейс командной строки для взаимодействия с Raft-кластером.
type CLI struct{}

// NewCLI создаёт новый экземпляр CLI.
func NewCLI() *CLI {
	return &CLI{}
}

// Run запускает REPL.
func (c *CLI) Run() error {
	return errors.New("not implemented")
}
