// Package backup keeps tenants' data, and the cluster's state, in an S3
// bucket: every backup the whole data of a tenant from a snapshot,
// compressed and encrypted, thinned out as it ages, and in production under
// object lock, which no one, not even a node holding the bucket's keys, can
// delete early.
package backup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Object is an object of the bucket.
type Object struct {
	Key          string
	Size         int64
	LastModified time.Time
}

// Store is the bucket backups go to.
type Store interface {
	// Put stores r at key; with retainUntil set, under compliance lock until
	// then.
	Put(ctx context.Context, key string, r io.Reader, retainUntil time.Time) error
	Get(ctx context.Context, key string) (io.ReadCloser, error)
	// List returns the current objects below prefix, by key.
	List(ctx context.Context, prefix string) ([]Object, error)
	// Delete removes every version of key; a locked one is refused.
	Delete(ctx context.Context, key string) error
	// Locked reports whether the bucket has object lock.
	Locked(ctx context.Context) (bool, error)
	// Create makes the bucket, with object lock or without.
	Create(ctx context.Context, locked bool) error
	// Exists reports whether the bucket is there.
	Exists(ctx context.Context) (bool, error)
	// Destroy removes every object version and the bucket.
	Destroy(ctx context.Context) error
}

// S3 is a bucket of an S3 service, such as Hetzner Object Storage.
type S3 struct {
	client *minio.Client
	bucket string
	region string
}

// NewS3 connects to bucket at endpoint (https://host) with the given keys.
func NewS3(endpoint, region, bucket, accessKey, secretKey string) (*S3, error) {
	secure := true
	host := endpoint
	switch {
	case len(endpoint) > 8 && endpoint[:8] == "https://":
		host = endpoint[8:]
	case len(endpoint) > 7 && endpoint[:7] == "http://":
		host, secure = endpoint[7:], false
	}
	client, err := minio.New(host, &minio.Options{Creds: credentials.NewStaticV4(accessKey, secretKey, ""), Secure: secure, Region: region})
	if err != nil {
		return nil, err
	}
	return &S3{client: client, bucket: bucket, region: region}, nil
}

func (s *S3) Put(ctx context.Context, key string, r io.Reader, retainUntil time.Time) error {
	options := minio.PutObjectOptions{ContentType: "application/octet-stream", PartSize: 16 << 20}
	info, err := s.client.PutObject(ctx, s.bucket, key, r, -1, options)
	if err != nil || retainUntil.IsZero() {
		return err
	}
	// A lock given with a multipart upload, which a stream of unknown length
	// is, may be dropped (Hetzner Object Storage drops it): the version
	// stored is locked on its own, and the lock read back.
	mode, until := minio.Compliance, retainUntil.UTC().Truncate(time.Second).Add(time.Second)
	locked := s.client.PutObjectRetention(ctx, s.bucket, key, minio.PutObjectRetentionOptions{Mode: &mode, RetainUntilDate: &until, VersionID: info.VersionID})
	if locked == nil {
		got, gotUntil, err := s.client.GetObjectRetention(ctx, s.bucket, key, info.VersionID)
		switch {
		case err != nil:
			locked = fmt.Errorf("read the lock back: %w", err)
		case got == nil || *got != minio.Compliance || gotUntil == nil || gotUntil.Before(retainUntil):
			locked = errors.New("the lock read back is not the one set")
		}
	}
	if locked != nil {
		// An unlocked backup would be taken for a locked one: it goes,
		// which it can, unlocked, and the backup is reported failed.
		removed := s.client.RemoveObject(context.WithoutCancel(ctx), s.bucket, key, minio.RemoveObjectOptions{VersionID: info.VersionID})
		if removed != nil {
			return fmt.Errorf("lock %s: %w; it stays unlocked, as removing it failed: %w", key, locked, removed)
		}
		return fmt.Errorf("lock %s: %w; it was removed", key, locked)
	}
	return nil
}

func (s *S3) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	object, err := s.client.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	// GetObject is lazy: fail here, not on the first read, for a missing key.
	if _, err := object.Stat(); err != nil {
		object.Close()
		return nil, err
	}
	return object, nil
}

func (s *S3) List(ctx context.Context, prefix string) ([]Object, error) {
	var objects []Object
	for info := range s.client.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if info.Err != nil {
			return nil, info.Err
		}
		objects = append(objects, Object{Key: info.Key, Size: info.Size, LastModified: info.LastModified})
	}
	return objects, nil
}

func (s *S3) Delete(ctx context.Context, key string) error {
	var errs []error
	for info := range s.client.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Prefix: key, Recursive: true, WithVersions: true}) {
		if info.Err != nil {
			return info.Err
		}
		if info.Key != key {
			continue
		}
		if err := s.client.RemoveObject(ctx, s.bucket, key, minio.RemoveObjectOptions{VersionID: info.VersionID}); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (s *S3) Locked(ctx context.Context) (bool, error) {
	enabled, _, _, _, err := s.client.GetObjectLockConfig(ctx, s.bucket)
	if err != nil {
		var response minio.ErrorResponse
		if errors.As(err, &response) && (response.Code == "ObjectLockConfigurationNotFoundError" || response.StatusCode == 404) {
			return false, nil
		}
		return false, err
	}
	return enabled == "Enabled", nil
}

// Create makes the bucket and returns once the service takes writes to
// it: a new bucket reaches the service's servers one after the other
// (Hetzner Object Storage's within a minute or so), so it waits for a run
// of writes that all land.
func (s *S3) Create(ctx context.Context, locked bool) error {
	if err := s.client.MakeBucket(ctx, s.bucket, minio.MakeBucketOptions{Region: s.region, ObjectLocking: locked}); err != nil {
		return err
	}
	const probe = "murder-bucket-ready"
	landed := 0
	for deadline := time.Now().Add(3 * time.Minute); landed < 5; {
		_, err := s.client.PutObject(ctx, s.bucket, probe, strings.NewReader("ready"), 5, minio.PutObjectOptions{})
		if err == nil {
			err = s.Delete(ctx, probe)
		}
		if err != nil {
			landed = 0
			if time.Now().After(deadline) {
				return fmt.Errorf("bucket %s made, but writes to it fail: %w", s.bucket, err)
			}
		} else {
			landed++
		}
		time.Sleep(time.Second)
	}
	return nil
}

func (s *S3) Exists(ctx context.Context) (bool, error) { return s.client.BucketExists(ctx, s.bucket) }

func (s *S3) Destroy(ctx context.Context) error {
	for info := range s.client.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Recursive: true, WithVersions: true}) {
		if info.Err != nil {
			return info.Err
		}
		if err := s.client.RemoveObject(ctx, s.bucket, info.Key, minio.RemoveObjectOptions{VersionID: info.VersionID}); err != nil {
			return fmt.Errorf("remove %s: %w", info.Key, err)
		}
	}
	return s.client.RemoveBucket(ctx, s.bucket)
}
