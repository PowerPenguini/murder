//go:build s3

package backup

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"strings"
	"testing"
	"time"
)

// These tests run against a real S3 service, such as Hetzner Object
// Storage: go test -tags s3 ./internal/backup, with MURDER_S3_ENDPOINT,
// MURDER_S3_REGION, MURDER_S3_ACCESS_FILE and MURDER_S3_SECRET_FILE set.
// They make buckets of their own and remove them.
func s3For(t *testing.T, bucket string) *S3 {
	t.Helper()
	read := func(name string) string {
		path := os.Getenv(name)
		if path == "" {
			t.Skip(name + " is not set")
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		return strings.TrimSpace(string(data))
	}
	access, secret := read("MURDER_S3_ACCESS_FILE"), read("MURDER_S3_SECRET_FILE")
	s3, err := NewS3(os.Getenv("MURDER_S3_ENDPOINT"), os.Getenv("MURDER_S3_REGION"), bucket, access, secret)
	if err != nil {
		t.Fatal(err)
	}
	return s3
}

func bucketName(kind string) string {
	suffix := make([]byte, 4)
	_, _ = rand.Read(suffix)
	return "murder-" + kind + "-" + hex.EncodeToString(suffix)
}

// A test bucket keeps no lock: its objects and the bucket go at once.
func TestATestBucketIsDeletedAtOnce(t *testing.T) {
	ctx := context.Background()
	s3 := s3For(t, bucketName("test"))
	if err := s3.Create(ctx, false); err != nil {
		t.Fatal(err)
	}
	if locked, err := s3.Locked(ctx); err != nil || locked {
		t.Fatalf("a test bucket locked: %v, %v", locked, err)
	}
	if err := s3.Put(ctx, "tenants/acme/x.tar.zst.age", strings.NewReader("backup"), time.Time{}); err != nil {
		t.Fatal(err)
	}
	if err := s3.Destroy(ctx); err != nil {
		t.Fatalf("a test bucket not destroyed: %v", err)
	}
	if exists, err := s3.Exists(ctx); err != nil || exists {
		t.Fatalf("the test bucket is still there: %v, %v", exists, err)
	}
}

// A production bucket keeps a locked object, against deletion of it and of
// the bucket, until its lock ends; then both go.
func TestAProductionBucketKeepsLockedBackupsUntilTheirLockEnds(t *testing.T) {
	ctx := context.Background()
	s3 := s3For(t, bucketName("locked"))
	if err := s3.Create(ctx, true); err != nil {
		t.Fatal(err)
	}
	if locked, err := s3.Locked(ctx); err != nil || !locked {
		t.Fatalf("a production bucket without lock: %v, %v", locked, err)
	}
	lock := 2 * time.Minute
	until := time.Now().Add(lock)
	key := "tenants/acme/locked.tar.zst.age"
	if err := s3.Put(ctx, key, strings.NewReader("backup"), until); err != nil {
		t.Fatalf("a locked put: %v", err)
	}
	if err := s3.Delete(ctx, key); err == nil {
		t.Fatal("a locked backup was deleted")
	} else {
		t.Logf("deleting a locked backup: %v", err)
	}
	if err := s3.Destroy(ctx); err == nil {
		t.Fatal("a bucket holding a locked backup was destroyed")
	}
	if objects, err := s3.List(ctx, "tenants/"); err != nil || len(objects) != 1 {
		t.Fatalf("after the refused deletions: %v, %v", objects, err)
	}
	time.Sleep(time.Until(until) + 20*time.Second)
	if err := s3.Destroy(ctx); err != nil {
		t.Fatalf("once its lock ended, the bucket was not destroyed: %v", err)
	}
}
