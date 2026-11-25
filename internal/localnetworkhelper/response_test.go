//go:build unix

package localnetworkhelper

import (
	"github.com/puzpuzpuz/xsync/v4"
	"golang.org/x/sys/unix"
	"net"
	"os"
	"testing"
	"time"
)

func TestSplitHelperErrorResponse(t *testing.T) {
	pair, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	connFile := os.NewFile(uintptr(pair[0]), "receiver")
	peerFile := os.NewFile(uintptr(pair[1]), "sender")
	conn, err := net.FileConn(connFile)
	if err != nil {
		t.Fatal(err)
	}
	peer, err := net.FileConn(peerFile)
	if err != nil {
		t.Fatal(err)
	}
	connFile.Close()
	peerFile.Close()
	defer conn.Close()
	defer peer.Close()
	h := &LocalNetworkHelper{unixConn: conn.(*net.UnixConn), requestsProcessed: xsync.NewMap[string, dialRequest]()}
	replyCh := make(chan dialReply, 1)
	h.requestsProcessed.Store("token", dialRequest{replyCh: replyCh})
	go h.handleResponses()
	_, err = peer.Write([]byte(`{"token":"tok`))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	_, err = peer.Write([]byte(`en","error":"expected"}`))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case reply := <-replyCh:
		if reply.err == nil || reply.err.Error() != "expected" {
			t.Fatalf("reply: %+v", reply)
		}
	case <-time.After(time.Second):
		if e := h.err.Load(); e != nil {
			t.Fatalf("reader failed on split response: %v", *e)
		}
		t.Fatal("response lost")
	}
}

func TestSplitHelperResponseWithDescriptor(t *testing.T) {
	pair, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	receiverFile := os.NewFile(uintptr(pair[0]), "receiver")
	senderFile := os.NewFile(uintptr(pair[1]), "sender")
	receiver, err := net.FileConn(receiverFile)
	if err != nil {
		t.Fatal(err)
	}
	sender, err := net.FileConn(senderFile)
	if err != nil {
		t.Fatal(err)
	}
	receiverFile.Close()
	senderFile.Close()
	defer receiver.Close()
	defer sender.Close()
	socket, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer socket.Close()
	raw, err := socket.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var rights []byte
	if err := raw.Control(func(fd uintptr) { rights = unix.UnixRights(int(fd)) }); err != nil {
		t.Fatal(err)
	}
	h := &LocalNetworkHelper{unixConn: receiver.(*net.UnixConn), requestsProcessed: xsync.NewMap[string, dialRequest]()}
	replyCh := make(chan dialReply, 1)
	h.requestsProcessed.Store("token", dialRequest{replyCh: replyCh})
	go h.handleResponses()
	_, _, err = sender.(*net.UnixConn).WriteMsgUnix([]byte(`{"token":"tok`), rights, nil)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-replyCh:
		t.Fatal("incomplete response delivered")
	case <-time.After(20 * time.Millisecond):
	}
	_, err = sender.Write([]byte(`en","error":""}`))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case reply := <-replyCh:
		if reply.err != nil || reply.conn == nil {
			t.Fatalf("reply: %+v", reply)
		}
		reply.conn.Close()
	case <-time.After(time.Second):
		if e := h.err.Load(); e != nil {
			t.Fatalf("reader failed on split response: %v", *e)
		}
		t.Fatal("response lost")
	}
}
