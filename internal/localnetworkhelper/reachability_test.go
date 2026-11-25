//go:build unix

package localnetworkhelper

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestOversizedDialErrorDoesNotStopResponseReader(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	helper, err := New(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer helper.Close()

	// The helper's dial error includes the invalid host and exceeds one
	// response read. The caller must receive that error, not time out.
	_, err = helper.PrivilegedDialContext(ctx, "tcp", strings.Repeat("a", 6000)+":1")
	if err == context.DeadlineExceeded {
		t.Fatal("helper lost the dial error response")
	}
	if err == nil || !strings.Contains(err.Error(), "no such host") {
		t.Fatalf("unexpected dial result: %v", err)
	}
}
