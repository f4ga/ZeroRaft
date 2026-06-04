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
	"testing"
)

func TestNewPcapWriter(t *testing.T) {
	path := t.TempDir() + "/test.pcap"
	pw, err := NewPcapWriter(path)
	if err != nil {
		t.Fatalf("NewPcapWriter failed: %v", err)
	}
	defer func() { _ = pw.Close() }()
	// Verify the file exists and has the correct global header size
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat failed: %v", err)
	}
	if info.Size() != PcapGlobalHeaderLen {
		t.Errorf("expected file size %d (global header), got %d", PcapGlobalHeaderLen, info.Size())
	}
}
func TestPcapWriterHeader(t *testing.T) {
	path := t.TempDir() + "/header.pcap"
	pw, err := NewPcapWriter(path)
	if err != nil {
		t.Fatalf("NewPcapWriter failed: %v", err)
	}
	_ = pw.Close()
	// Read back and verify the global header
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file failed: %v", err)
	}
	if len(data) < PcapGlobalHeaderLen {
		t.Fatalf("file too short: %d bytes", len(data))
	}
	// Check magic number
	magic := binary.LittleEndian.Uint32(data[0:4])
	if magic != 0xa1b2c3d4 {
		t.Errorf("magic number: got 0x%08x, want 0xa1b2c3d4", magic)
	}
	// Check version
	major := binary.LittleEndian.Uint16(data[4:6])
	minor := binary.LittleEndian.Uint16(data[6:8])
	if major != 2 || minor != 4 {
		t.Errorf("version: got %d.%d, want 2.4", major, minor)
	}
	// Check snaplen
	snaplen := binary.LittleEndian.Uint32(data[16:20])
	if snaplen != DefaultSnaplen {
		t.Errorf("snaplen: got %d, want %d", snaplen, DefaultSnaplen)
	}
	// Check link type (DLT_RAW = 101)
	linkType := binary.LittleEndian.Uint32(data[20:24])
	if linkType != 101 {
		t.Errorf("link type: got %d, want 101 (DLT_RAW)", linkType)
	}
}
func TestPcapWritePacket(t *testing.T) {
	path := t.TempDir() + "/packet.pcap"
	pw, err := NewPcapWriter(path)
	if err != nil {
		t.Fatalf("NewPcapWriter failed: %v", err)
	}
	// Write a test packet
	packetData := []byte("hello pcap test packet")
	if err := pw.WritePacket(packetData); err != nil {
		t.Fatalf("WritePacket failed: %v", err)
	}
	_ = pw.Close()
	// Read back and verify
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file failed: %v", err)
	}
	expectedLen := PcapGlobalHeaderLen + PcapPacketHeaderLen + len(packetData)
	if len(data) != expectedLen {
		t.Fatalf("expected file size %d, got %d", expectedLen, len(data))
	}
	// Verify packet header
	inclLen := binary.LittleEndian.Uint32(data[PcapGlobalHeaderLen+8 : PcapGlobalHeaderLen+12])
	origLen := binary.LittleEndian.Uint32(data[PcapGlobalHeaderLen+12 : PcapGlobalHeaderLen+16])
	if inclLen != uint32(len(packetData)) {
		t.Errorf("incl_len: got %d, want %d", inclLen, len(packetData))
	}
	if origLen != uint32(len(packetData)) {
		t.Errorf("orig_len: got %d, want %d", origLen, len(packetData))
	}
	// Verify packet data
	packetStart := PcapGlobalHeaderLen + PcapPacketHeaderLen
	packetContent := data[packetStart : packetStart+len(packetData)]
	if string(packetContent) != string(packetData) {
		t.Errorf("packet data: got %q, want %q", packetContent, packetData)
	}
}
func TestPcapWriteMultiplePackets(t *testing.T) {
	path := t.TempDir() + "/multi.pcap"
	pw, err := NewPcapWriter(path)
	if err != nil {
		t.Fatalf("NewPcapWriter failed: %v", err)
	}
	packets := [][]byte{
		[]byte("packet one"),
		[]byte("packet two with more data"),
		[]byte("pkt3"),
	}
	for _, pkt := range packets {
		if err := pw.WritePacket(pkt); err != nil {
			t.Fatalf("WritePacket failed: %v", err)
		}
	}
	_ = pw.Close()
	// Read back and verify
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file failed: %v", err)
	}
	offset := PcapGlobalHeaderLen
	for i, expected := range packets {
		if offset+PcapPacketHeaderLen > len(data) {
			t.Fatalf("packet %d: file truncated at offset %d", i, offset)
		}
		inclLen := binary.LittleEndian.Uint32(data[offset+8 : offset+12])
		if int(inclLen) != len(expected) {
			t.Errorf("packet %d: incl_len %d, want %d", i, inclLen, len(expected))
		}
		packetStart := offset + PcapPacketHeaderLen
		if packetStart+int(inclLen) > len(data) {
			t.Fatalf("packet %d: data truncated at offset %d", i, packetStart)
		}
		got := string(data[packetStart : packetStart+int(inclLen)])
		if got != string(expected) {
			t.Errorf("packet %d: got %q, want %q", i, got, expected)
		}
		offset += PcapPacketHeaderLen + int(inclLen)
	}
}
func TestPcapClose(t *testing.T) {
	path := t.TempDir() + "/close.pcap"
	pw, err := NewPcapWriter(path)
	if err != nil {
		t.Fatalf("NewPcapWriter failed: %v", err)
	}
	// Close should succeed
	if err := pw.Close(); err != nil {
		t.Errorf("first Close failed: %v", err)
	}
	// Second close may return an error (file already closed), but should not panic
	_ = pw.Close()
}
func TestPcapConcurrentWrites(t *testing.T) {
	path := t.TempDir() + "/concurrent.pcap"
	pw, err := NewPcapWriter(path)
	if err != nil {
		t.Fatalf("NewPcapWriter failed: %v", err)
	}
	defer func() { _ = pw.Close() }()
	const workers = 10
	const writesPerWorker = 100
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(workerID int) {
			defer wg.Done()
			data := []byte("test packet")
			for j := 0; j < writesPerWorker; j++ {
				if err := pw.WritePacket(data); err != nil {
					t.Errorf("worker %d write error: %v", workerID, err)
				}
			}
		}(i)
	}
	wg.Wait()
	// Verify the file exists and has a reasonable size
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() == 0 {
		t.Error("pcap file is empty")
	}
}
func TestPcapSync(t *testing.T) {
	path := t.TempDir() + "/sync.pcap"
	pw, err := NewPcapWriter(path)
	if err != nil {
		t.Fatalf("NewPcapWriter failed: %v", err)
	}
	defer func() { _ = pw.Close() }()
	if err := pw.Sync(); err != nil {
		t.Errorf("Sync failed: %v", err)
	}
}
func TestPcapNewWriterInvalidPath(t *testing.T) {
	_, err := NewPcapWriter("/nonexistent/dir/test.pcap")
	if err == nil {
		t.Error("expected error for invalid path")
	}
}
