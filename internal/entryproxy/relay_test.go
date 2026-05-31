package entryproxy

import (
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

func tcpPair(t *testing.T) (net.Conn, net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	type res struct {
		c net.Conn
		e error
	}
	ch := make(chan res, 1)
	go func() { c, e := ln.Accept(); ch <- res{c, e} }()
	dialed, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	r := <-ch
	if r.e != nil {
		t.Fatal(r.e)
	}
	t.Cleanup(func() { _ = dialed.Close(); _ = r.c.Close() })
	return dialed, r.c
}

func closeWrite(t *testing.T, c net.Conn) {
	t.Helper()
	if cw, ok := c.(interface{ CloseWrite() error }); ok {
		_ = cw.CloseWrite()
		return
	}
	t.Fatal("conn has no CloseWrite")
}

// TestRelayHalfCloseKeepsDownloadAlive proves a client half-closing its upload
// (done sending) must not tear down the still-flowing download.
func TestRelayHalfCloseKeepsDownloadAlive(t *testing.T) {
	clientPeer, client := tcpPair(t) // client = entry's view of the user conn
	backend, backendPeer := tcpPair(t)

	go relay(client, backend, &connStat{})

	if _, err := clientPeer.Write([]byte("req")); err != nil {
		t.Fatal(err)
	}
	closeWrite(t, clientPeer) // user finished uploading, still expects the response

	got := make([]byte, 3)
	_ = backendPeer.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.ReadFull(backendPeer, got); err != nil || string(got) != "req" {
		t.Fatalf("backend did not get request: %q %v", got, err)
	}
	// The upload half-close must surface as EOF here, not a full reset.
	if _, err := backendPeer.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF after client half-close, got %v", err)
	}
	if _, err := backendPeer.Write([]byte("DOWNLOAD")); err != nil {
		t.Fatalf("backend send after half-close failed (conn was torn down): %v", err)
	}
	closeWrite(t, backendPeer)

	_ = clientPeer.SetReadDeadline(time.Now().Add(2 * time.Second))
	dl := make([]byte, 8)
	n, err := io.ReadFull(clientPeer, dl)
	if err != nil || string(dl[:n]) != "DOWNLOAD" {
		t.Fatalf("download killed by upload half-close: got %q err %v", dl[:n], err)
	}
}
