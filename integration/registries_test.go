//go:build docker

package integration

import (
	"context"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"

	"github.com/PowerPenguini/murder/internal/runtime"
)

// A private registry is pulled from with the login the agent is given: a
// user and a token in a file.
func TestPullFromAPrivateRegistryOnDocker(t *testing.T) {
	dir := t.TempDir()
	hash, err := bcrypt.GenerateFromPassword([]byte("s3cret-token"), bcrypt.DefaultCost)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "htpasswd"), []byte("bot:"+string(hash)+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	output, err := exec.Command("docker", "run", "-d", "--rm", "-p", "127.0.0.1::5000", "-v", dir+":/auth:ro",
		"-e", "REGISTRY_AUTH=htpasswd", "-e", "REGISTRY_AUTH_HTPASSWD_REALM=murder", "-e", "REGISTRY_AUTH_HTPASSWD_PATH=/auth/htpasswd", "registry:2").Output()
	if err != nil {
		t.Fatalf("start a registry: %v\n%s", err, output)
	}
	id := strings.TrimSpace(string(output))
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", id).Run() })
	output, err = exec.Command("docker", "port", id, "5000/tcp").Output()
	if err != nil {
		t.Fatalf("the registry's port: %v", err)
	}
	host := strings.Replace(strings.TrimSpace(strings.Split(string(output), "\n")[0]), "127.0.0.1", "localhost", 1)
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(200 * time.Millisecond) {
		if response, err := http.Get("http://" + host + "/v2/"); err == nil {
			response.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the registry does not answer")
		}
	}

	// The docker CLI signs in with a config of its own, not the user's.
	cli := func(args ...string) string {
		t.Helper()
		command := exec.Command("docker", args...)
		command.Env = append(os.Environ(), "DOCKER_CONFIG="+filepath.Join(dir, "cli"))
		command.Stdin = strings.NewReader("s3cret-token")
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("docker %v: %v\n%s", args, err, output)
		}
		return strings.TrimSpace(string(output))
	}
	// No credential helper either: the login must not reach the user's
	// keyring.
	if err := os.MkdirAll(filepath.Join(dir, "cli"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "cli", "config.json"), []byte(`{"credsStore":""}`), 0o600); err != nil {
		t.Fatal(err)
	}
	ref := host + "/murder-it/private:1"
	imageID := buildTestImage(t, "private", ref)
	cli("login", host, "-u", "bot", "--password-stdin")
	cli("push", "-q", ref)
	pinned := cli("image", "inspect", "--format", "{{index .RepoDigests 0}}", ref)
	// Gone from the engine, so pulling has to fetch it.
	cli("image", "rm", "-f", ref, imageID)

	anonymous := runtime.NewDocker(runtime.DockerOptions{Socket: dockerSocket(), Node: "registries-it"})
	if err := anonymous.PullImage(context.Background(), pinned); err == nil {
		t.Fatal("a private image was pulled without a login")
	}
	tokenFile := filepath.Join(dir, "registry.token")
	if err := os.WriteFile(tokenFile, []byte("s3cret-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	signedIn := runtime.NewDocker(runtime.DockerOptions{Socket: dockerSocket(), Node: "registries-it", Registries: []runtime.RegistryLogin{{Host: host, Username: "bot", TokenFile: tokenFile}}})
	if err := signedIn.PullImage(context.Background(), pinned); err != nil {
		t.Fatalf("pull with the login: %v", err)
	}
	t.Cleanup(func() { _ = exec.Command("docker", "image", "rm", "-f", pinned).Run() })
	if present, err := signedIn.HasImage(context.Background(), pinned); err != nil || !present {
		t.Fatalf("the pulled image is not there: %v %v", present, err)
	}
}
