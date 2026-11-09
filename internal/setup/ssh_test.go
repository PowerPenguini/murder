package setup

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
)

const sshdTakesPasswords = "port 22\nusepam yes\npasswordauthentication yes\nkbdinteractiveauthentication no\nauthenticationmethods any\nauthorizedkeysfile .ssh/authorized_keys .ssh/authorized_keys2\nauthorizedkeyscommand none\n"

// sshHost is a host whose sshd reads the drop-in: with it, passwords are
// off, unless an earlier setting keeps them on.
type sshHost struct {
	*fakeHost
	earlier bool // a setting before the drop-in turns passwords on
	refuse  bool // sshd refuses its configuration with the drop-in
}

func (h *sshHost) Run(ctx context.Context, name string, args ...string) (string, error) {
	if name == "sshd" && slices.Equal(args, []string{"-T"}) {
		h.ran = append(h.ran, "sshd -T")
		if _, ok := h.files[SSHDropIn]; ok {
			switch {
			case h.refuse:
				return "", errors.New("Bad configuration option")
			case !h.earlier:
				return strings.Replace(sshdTakesPasswords, "passwordauthentication yes", "passwordauthentication no", 1), nil
			}
		}
		return sshdTakesPasswords, nil
	}
	return h.fakeHost.Run(ctx, name, args...)
}

func newSSHHost() *sshHost {
	host := &sshHost{fakeHost: newFakeHost()}
	host.commands["getent passwd ubuntu"] = "ubuntu:x:1000:1000:Ubuntu:/home/ubuntu:/bin/bash\n"
	host.commands["systemctl try-reload-or-restart ssh.service"] = ""
	return host
}

func TestSetupTurnsSSHPasswordsOffForWhoeverLogsInWithAKey(t *testing.T) {
	host := newSSHHost()
	host.files["/home/ubuntu/.ssh/authorized_keys2"] = []byte("# keys\n\nssh-ed25519 AAAA ubuntu@laptop\n")
	facts := Inspect(context.Background(), host)
	if !facts.SSH.Harden() || facts.SSH.Keys != "/home/ubuntu/.ssh/authorized_keys2" || facts.SSH.Account != "ubuntu" {
		t.Fatalf("ssh facts %+v", facts.SSH)
	}
	if warnings := strings.Join(facts.Warnings(), "\n"); strings.Contains(warnings, "SSH") {
		t.Fatalf("setup turns passwords off, yet warns: %s", warnings)
	}
	steps, err := Steps(host, Install{SSH: facts.SSH.Keys}, false, func() error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	var step *Step
	for i := range steps {
		if strings.Contains(steps[i].Say, "SSH") {
			step = &steps[i]
		}
	}
	if step == nil || !strings.Contains(step.Say, facts.SSH.Keys) {
		t.Fatalf("no SSH step naming the key in %v", steps)
	}
	if err := step.Do(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := string(host.files[SSHDropIn]); !strings.Contains(got, "PasswordAuthentication no") || host.modes[SSHDropIn] != 0o644 {
		t.Fatalf("drop-in %q, mode %v", got, host.modes[SSHDropIn])
	}
	if !slices.Contains(host.ran, "systemctl try-reload-or-restart ssh.service") {
		t.Fatalf("sshd was not reloaded: %v", host.ran)
	}
}

func TestSetupLeavesSSHAloneWithoutAKeyToLogInWith(t *testing.T) {
	host := newSSHHost()
	host.files["/home/ubuntu/.ssh/authorized_keys"] = []byte("# no keys yet\n\n")
	facts := Inspect(context.Background(), host)
	if facts.SSH.Harden() || !facts.SSH.Passwords {
		t.Fatalf("ssh facts %+v", facts.SSH)
	}
	if warnings := strings.Join(facts.Warnings(), "\n"); !strings.Contains(warnings, "ubuntu has no key") {
		t.Fatalf("warnings %q", warnings)
	}

	// Logins that need a key and a password stay as they are.
	host = newSSHHost()
	host.files["/home/ubuntu/.ssh/authorized_keys"] = []byte("ssh-ed25519 AAAA\n")
	twoFactor := &twoFactorHost{host}
	if facts := Inspect(context.Background(), twoFactor); facts.SSH.Harden() || !strings.Contains(strings.Join(facts.Warnings(), "\n"), "AuthenticationMethods") {
		t.Fatalf("two factors: %+v, warnings %v", facts.SSH, facts.Warnings())
	}

	// Without passwords there is nothing to do, and nothing to say.
	host = newSSHHost()
	keysOnly := &keysOnlyHost{host}
	if facts := Inspect(context.Background(), keysOnly); facts.SSH.Passwords || len(facts.Warnings()) != 0 {
		t.Fatalf("keys only: %+v, warnings %v", facts.SSH, facts.Warnings())
	}
}

type twoFactorHost struct{ *sshHost }

func (h *twoFactorHost) Run(ctx context.Context, name string, args ...string) (string, error) {
	out, err := h.sshHost.Run(ctx, name, args...)
	return strings.Replace(out, "authenticationmethods any", "authenticationmethods publickey,keyboard-interactive", 1), err
}

type keysOnlyHost struct{ *sshHost }

func (h *keysOnlyHost) Run(ctx context.Context, name string, args ...string) (string, error) {
	out, err := h.sshHost.Run(ctx, name, args...)
	return strings.Replace(out, "passwordauthentication yes", "passwordauthentication no", 1), err
}

func TestSetupTakesItsSSHSettingAwayWhenItDoesNotTakeHold(t *testing.T) {
	for name, host := range map[string]*sshHost{
		"an earlier setting": {fakeHost: newSSHHost().fakeHost, earlier: true},
		"a refused config":   {fakeHost: newSSHHost().fakeHost, refuse: true},
	} {
		var skipped Skipped
		if err := hardenSSH(context.Background(), host); !errors.As(err, &skipped) {
			t.Fatalf("%s: %v", name, err)
		}
		if _, ok := host.files[SSHDropIn]; ok {
			t.Fatalf("%s: the drop-in stayed", name)
		}
		if slices.ContainsFunc(host.ran, func(line string) bool { return strings.Contains(line, "reload") }) {
			t.Fatalf("%s: sshd was reloaded: %v", name, host.ran)
		}
	}
}
