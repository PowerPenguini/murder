package hostprobe

import (
	"context"
	"errors"
	"os"
	"path/filepath"
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

const (
	sshSocket  = "systemctl show -p LoadState -p Listen ssh.socket"
	sshdSocket = "systemctl show -p LoadState -p Listen sshd.socket"
	notFound   = "LoadState=not-found\nListen=\n"
)

// SSHPorts says when it could not ask sshd, its socket units or the
// sockets, with the ports it found all the same, and never makes up 22.
func TestSSHPortsSayWhatCouldNotBeAsked(t *testing.T) {
	complete := runner{"sshd -T": "port 22\n", sshSocket: notFound, sshdSocket: notFound,
		"ss -H -ltunp": "tcp LISTEN 0 128 0.0.0.0:2222 0.0.0.0:* users:((\"sshd\",pid=1,fd=3))\n"}
	if ports, err := SSHPorts(context.Background(), complete); err != nil || !slices.Equal(ports, []int{22, 2222}) {
		t.Fatalf("complete: %v, %v", ports, err)
	}
	noSS := runner{"sshd -T": "port 22\n", sshSocket: notFound, sshdSocket: notFound}
	if ports, err := SSHPorts(context.Background(), noSS); err == nil || !strings.Contains(err.Error(), "ss") || !slices.Equal(ports, []int{22}) {
		t.Fatalf("without ss: %v, %v", ports, err)
	}
	if ports, err := SSHPorts(context.Background(), runner{}); err == nil || len(ports) != 0 {
		t.Fatalf("nothing answers: %v, %v", ports, err)
	}
}

// A socket-activated ssh: the socket unit holds 2222 before any sshd does,
// and sshd -T says 22. Both are SSH's; a socket unit that cannot be asked
// leaves the detection incomplete, and one that does not exist does not.
func TestSSHPortsOfASocketUnit(t *testing.T) {
	r := runner{"sshd -T": "port 22\n", "ss -H -ltunp": "tcp LISTEN 0 4096 *:2222 *:* users:((\"systemd\",pid=1,fd=50))\n",
		sshSocket: "LoadState=loaded\nListen=[::]:2222 (Stream)\nListen=0.0.0.0:2223 (Stream)\n", sshdSocket: notFound}
	if ports, err := SSHPorts(context.Background(), r); err != nil || !slices.Equal(ports, []int{22, 2222, 2223}) {
		t.Fatalf("socket unit: %v, %v", ports, err)
	}
	// Masked, it holds nothing and is no failure.
	r[sshSocket] = "LoadState=masked\nListen=\n"
	if ports, err := SSHPorts(context.Background(), r); err != nil || !slices.Equal(ports, []int{22}) {
		t.Fatalf("masked: %v, %v", ports, err)
	}
	for name, answer := range map[string]string{"failed": "", "no state": "Listen=[::]:2222 (Stream)\n", "bad setting": "LoadState=bad-setting\n"} {
		r := runner{"sshd -T": "port 22\n", "ss -H -ltunp": "", sshdSocket: notFound}
		if name != "failed" {
			r[sshSocket] = answer
		}
		if ports, err := SSHPorts(context.Background(), r); err == nil || !strings.Contains(err.Error(), "ssh.socket") || !slices.Equal(ports, []int{22}) {
			t.Errorf("%s: %v, %v", name, ports, err)
		}
	}
}

// The records are root's alone and read strictly: a corrupt, empty or
// unreadable one is nothing recorded, never half a record.
func TestRecords(t *testing.T) {
	system := System{StateDir: t.TempDir()}
	if _, err := system.RecordedSSHPorts(); !errors.Is(err, ErrNothingRecorded) {
		t.Fatalf("no record: %v", err)
	}
	if err := system.RecordSSHPorts([]int{22, 2222}); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(system.StateDir, "ssh-ports")
	if info, err := os.Stat(file); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("record %v, %v", info.Mode(), err)
	}
	if ports, err := system.RecordedSSHPorts(); err != nil || !slices.Equal(ports, []int{22, 2222}) {
		t.Fatalf("recorded %v, %v", ports, err)
	}
	for _, corrupt := range []string{"", "\n", "22 2222", "22  2222\n", "22\n2222\n", "22 x\n", "22 70000\n", "022\n", "-1\n", "\x00\x00"} {
		if err := os.WriteFile(file, []byte(corrupt), 0o600); err != nil {
			t.Fatal(err)
		}
		if ports, err := system.RecordedSSHPorts(); !errors.Is(err, ErrNothingRecorded) || ports != nil {
			t.Errorf("corrupt %q: %v, %v", corrupt, ports, err)
		}
	}
	if err := system.RecordPublicAddresses([]string{"203.0.113.10", "198.51.100.7"}); err != nil {
		t.Fatal(err)
	}
	if addresses, err := system.RecordedPublicAddresses(); err != nil || !slices.Equal(addresses, []string{"203.0.113.10", "198.51.100.7"}) {
		t.Fatalf("addresses %v, %v", addresses, err)
	}
	if info, err := os.Stat(filepath.Join(system.StateDir, "public-addresses")); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("public record %v, %v", info.Mode(), err)
	}
	for _, corrupt := range []string{"", "203.0.113.10", "10.0.0.2\n", "example.com\n", "2001:db8::1\n", "203.0.113.10  198.51.100.7\n"} {
		if _, err := ParseAddressesRecord(corrupt); !errors.Is(err, ErrNothingRecorded) {
			t.Errorf("corrupt %q: %v", corrupt, err)
		}
	}
}

func TestFormatPorts(t *testing.T) {
	if text := FormatPorts([]int{22, 2222}); text != "22 2222\n" {
		t.Fatalf("format %q", text)
	}
}
