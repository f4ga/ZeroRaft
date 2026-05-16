package raft

import "errors"

// Raft представляет узел Raft-кластера.
type Raft struct {
	ID   uint64
	Addr string
}

// NewRaft создаёт новый экземпляр Raft-узла.
func NewRaft(id uint64, addr string) *Raft {
	return &Raft{
		ID:   id,
		Addr: addr,
	}
}

// Start запускает Raft-узел.
func (r *Raft) Start() error {
	return errors.New("not implemented")
}
