package store

import (
	"os"
	"testing"
)

// Every transaction of these tests also reads the whole state and fails if
// what it read again in part differs.
func TestMain(m *testing.M) {
	verifyReloads = true
	os.Exit(m.Run())
}
