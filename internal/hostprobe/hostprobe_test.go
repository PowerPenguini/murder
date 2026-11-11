package hostprobe

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
)

// runner answers commands from a table; missing ones fail.
type runner map[string]string

func (r runner) Run(_ context.Context, name string, args ...string) (string, error) {
	out, ok := r[strings.TrimSpace(name+" "+strings.Join(args, " "))]
	if !ok {
		return "", errors.New(name + ": not found")
	}
	return out, nil
}

// SSHPorts says when it could not ask sshd or read the sockets, with the
// ports it found all the same, and never makes up 22 itself.
func TestSSHPortsSayWhatCouldNotBeAsked(t *testing.T) {
	complete := runner{"sshd -T": "port 22\n", "ss -H -ltunp": "tcp LISTEN 0 128 0.0.0.0:2222 0.0.0.0:* users:((\"sshd\",pid=1,fd=3))\n"}
	if ports, err := SSHPorts(context.Background(), complete); err != nil || !slices.Equal(ports, []int{22, 2222}) {
		t.Fatalf("complete: %v, %v", ports, err)
	}
	noSS := runner{"sshd -T": "port 22\n"}
	if ports, err := SSHPorts(context.Background(), noSS); err == nil || !strings.Contains(err.Error(), "ss") || !slices.Equal(ports, []int{22}) {
		t.Fatalf("without ss: %v, %v", ports, err)
	}
	if ports, err := SSHPorts(context.Background(), runner{}); err == nil || len(ports) != 0 {
		t.Fatalf("nothing answers: %v, %v", ports, err)
	}
}

func TestPortsRecord(t *testing.T) {
	if text := FormatPorts([]int{22, 2222}); text != "22 2222\n" {
		t.Fatalf("format %q", text)
	}
	if ports := ParsePorts("2222 22 x 70000 22\n"); !slices.Equal(ports, []int{22, 2222}) {
		t.Fatalf("parse %v", ports)
	}
}
