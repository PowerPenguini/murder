//go:build docker

package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// An image that declares a volume inside the data path gets no anonymous
// volume of the engine's once that path is bound from the data: what the
// app writes there stays in the tenant's data and outlives the container.
// Without such a bind the container is refused, before the engine makes a
// volume that Remove would destroy.
func TestAnImageVolumeIsNeverAnAnonymousVolume(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("Docker is not installed")
	}
	build := exec.Command("docker", "build", "-q", "-")
	build.Stdin = strings.NewReader("FROM scratch\nVOLUME /data/child\nCMD [\"/missing\"]\n")
	output, err := build.Output()
	if err != nil {
		t.Fatalf("build the image: %v", err)
	}
	image := strings.TrimSpace(string(output))
	t.Cleanup(func() { _ = exec.Command("docker", "image", "rm", image).Run() })
	socket := os.Getenv("MURDER_DOCKER_SOCKET")
	if socket == "" {
		socket = "/var/run/docker.sock"
	}
	d := NewDocker(DockerOptions{Socket: socket, Node: "volume-test"})
	ctx := context.Background()
	if volumes, err := d.ImageVolumes(ctx, image); err != nil || len(volumes) != 1 || volumes[0] != "/data/child" {
		t.Fatalf("ImageVolumes = %v, %v", volumes, err)
	}
	data := t.TempDir()
	spec := ContainerSpec{Name: "murder-volume-test", Image: image, User: "10001:10001", Network: "bridge", Mounts: []Mount{{Source: data, Target: "/data"}}, ReadOnlyRoot: true}
	if id, err := d.Create(ctx, spec); !errors.Is(err, ErrUncoveredVolume) {
		_ = d.Remove(ctx, id)
		t.Fatalf("Create with the volume uncovered = %q, %v", id, err)
	}
	if err := os.Mkdir(filepath.Join(data, "child"), 0o700); err != nil {
		t.Fatal(err)
	}
	spec.Mounts = append(spec.Mounts, Mount{Source: filepath.Join(data, "child"), Target: "/data/child"})
	id, err := d.Create(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Remove(context.Background(), id) })
	output, err = exec.Command("docker", "inspect", id).Output()
	if err != nil {
		t.Fatal(err)
	}
	var inspected []struct {
		Mounts []struct{ Type, Source, Destination string }
	}
	if err := json.Unmarshal(output, &inspected); err != nil {
		t.Fatal(err)
	}
	for _, mount := range inspected[0].Mounts {
		if mount.Type != "bind" {
			t.Errorf("the container has a %s mount at %s, want binds only", mount.Type, mount.Destination)
		}
	}
	if len(inspected[0].Mounts) != 2 {
		t.Fatalf("mounts = %+v", inspected[0].Mounts)
	}
}
