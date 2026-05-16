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
