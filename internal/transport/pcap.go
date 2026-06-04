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
	"encoding/binary"
	"os"
	"sync"
	"time"
)

// PcapGlobalHeaderLen is the length of the PCAP global header (24 bytes).
const PcapGlobalHeaderLen = 24

// PcapPacketHeaderLen is the length of the PCAP per-packet header (16 bytes).
const PcapPacketHeaderLen = 16

// DefaultSnaplen is the default snapshot length for PCAP.
const DefaultSnaplen = 65535

// PcapWriter writes packets in PCAP format (pcap, not pcapng).
// The file can be opened with Wireshark, tcpdump, etc.
type PcapWriter struct {
	file *os.File
	mu   sync.Mutex
}

// NewPcapWriter creates a new PCAP file and writes the global header.
// The file uses DLT_RAW (link type 101) for raw IP packets.
func NewPcapWriter(filename string) (*PcapWriter, error) {
	f, err := os.Create(filename)
	if err != nil {
		return nil, err
	}
	// Write PCAP global header (24 bytes).
	// Format: magic_number(4), version_major(2), version_minor(2),
	//         thiszone(4), sigfigs(4), snaplen(4), network(4).
	header := make([]byte, PcapGlobalHeaderLen)
	binary.LittleEndian.PutUint32(header[0:4], 0xa1b2c3d4)       // magic number
	binary.LittleEndian.PutUint16(header[4:6], 2)                // version major
	binary.LittleEndian.PutUint16(header[6:8], 4)                // version minor
	binary.LittleEndian.PutUint32(header[8:12], 0)               // thiszone (GMT)
	binary.LittleEndian.PutUint32(header[12:16], 0)              // sigfigs
	binary.LittleEndian.PutUint32(header[16:20], DefaultSnaplen) // snaplen
	binary.LittleEndian.PutUint32(header[20:24], 101)            // link type: DLT_RAW
	if _, err := f.Write(header); err != nil {
		_ = f.Close()
		return nil, err
	}
	return &PcapWriter{file: f}, nil
}

// WritePacket writes a single packet to the PCAP file.
// It prepends a 16-byte packet header with timestamp, length, and original length.
func (p *PcapWriter) WritePacket(data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	sec := uint32(now.Unix())
	usec := uint32(now.Nanosecond() / 1000) // microseconds
	// Packet header: ts_sec(4), ts_usec(4), incl_len(4), orig_len(4)
	hdr := make([]byte, PcapPacketHeaderLen)
	binary.LittleEndian.PutUint32(hdr[0:4], sec)
	binary.LittleEndian.PutUint32(hdr[4:8], usec)
	binary.LittleEndian.PutUint32(hdr[8:12], uint32(len(data)))
	binary.LittleEndian.PutUint32(hdr[12:16], uint32(len(data)))
	if _, err := p.file.Write(hdr); err != nil {
		return err
	}
	_, err := p.file.Write(data)
	return err
}

// Close closes the PCAP file.
func (p *PcapWriter) Close() error {
	return p.file.Close()
}

// Sync flushes the PCAP file to disk.
func (p *PcapWriter) Sync() error {
	return p.file.Sync()
}
