package setup

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
)

// SSHDropIn turns SSH's password logins off. sshd takes the first value
// it reads of a setting, and reads its drop-ins in name order, so this
// one comes before those of cloud-init (50-cloud-init.conf), which often
// turn passwords on.
const SSHDropIn = "/etc/ssh/sshd_config.d/00-murder.conf"

const sshDropIn = `# Written by "murder setup": SSH takes keys only, no passwords.
# sshd takes the first value it reads, so this file comes first.
PasswordAuthentication no
KbdInteractiveAuthentication no
`

// SSH is what sshd lets in, and whether whoever runs setup could still
// log in with SSH's passwords off.
type SSH struct {
	// Passwords reports that sshd takes passwords: plain ones, or
	// keyboard-interactive ones PAM asks for.
	Passwords bool
	// Keys is the file the account running setup logs in with a key
	// from; empty if it has none.
	Keys string
	// Account runs setup: the one sudo was run from, or root.
	Account string
	// Leave is why setup leaves sshd as it is, though it takes passwords.
	Leave string
}

// Harden reports whether setup turns SSH's passwords off: it takes them,
// and the account running setup logs in with a key.
func (s SSH) Harden() bool { return s.Passwords && s.Keys != "" && s.Leave == "" }

// inspectSSH reads sshd's effective settings, and looks for a key the
// account running setup logs in with.
func inspectSSH(ctx context.Context, host Host) SSH {
	settings, err := sshdSettings(ctx, host)
	if err != nil {
		return SSH{}
	}
	s := SSH{Account: host.Invoker()}
	s.Passwords = settings["passwordauthentication"] == "yes" || settings["kbdinteractiveauthentication"] == "yes" && settings["usepam"] == "yes"
	if !s.Passwords {
		return s
	}
	switch {
	case settings["authenticationmethods"] != "" && settings["authenticationmethods"] != "any":
		// Logins that need a key and a password, or a second factor PAM
		// asks for: turning passwords off would lock everyone out.
		s.Leave = "sshd's AuthenticationMethods asks for more than a key (" + settings["authenticationmethods"] + ")"
		return s
	case settings["authorizedkeyscommand"] != "" && settings["authorizedkeyscommand"] != "none":
		s.Leave = "sshd takes keys from an AuthorizedKeysCommand, which setup cannot read"
		return s
	}
	home, err := homeOf(ctx, host, s.Account)
	if err != nil {
		s.Leave = err.Error()
		return s
	}
	for _, file := range strings.Fields(settings["authorizedkeysfile"]) {
		file = strings.NewReplacer("%h", home, "%u", s.Account, "%%", "%").Replace(file)
		if !path.IsAbs(file) {
			file = path.Join(home, file)
		}
		if data, err := host.ReadFile(file); err == nil && hasKey(string(data)) {
			s.Keys = file
			break
		}
	}
	return s
}

// sshdSettings are sshd's effective settings, by lowercase name.
func sshdSettings(ctx context.Context, host Host) (map[string]string, error) {
	output, err := host.Run(ctx, "sshd", "-T")
	if err != nil {
		if output, err = host.Run(ctx, "/usr/sbin/sshd", "-T"); err != nil {
			return nil, err
		}
	}
	settings := map[string]string{}
	for line := range strings.Lines(output) {
		key, value, _ := strings.Cut(strings.TrimSpace(line), " ")
		settings[key] = value
	}
	return settings, nil
}

// homeOf is an account's home directory.
func homeOf(ctx context.Context, host Host, account string) (string, error) {
	entry, err := host.Run(ctx, "getent", "passwd", account)
	fields := strings.Split(strings.TrimSpace(entry), ":")
	if err != nil || len(fields) < 6 || !path.IsAbs(fields[5]) {
		return "", fmt.Errorf("cannot find the home of %s", account)
	}
	return fields[5], nil
}

// hasKey reports whether an authorized_keys file holds a key: a line
// that is neither blank nor a comment.
func hasKey(data string) bool {
	for line := range strings.Lines(data) {
		if line = strings.TrimSpace(line); line != "" && !strings.HasPrefix(line, "#") {
			return true
		}
	}
	return false
}

// hardenSSH turns SSH's password logins off, for new logins: the ones
// open now stay. It takes the drop-in away again if sshd refuses it, or
// if an earlier setting still turns passwords on.
func hardenSSH(ctx context.Context, host Host) error {
	if err := host.MkdirAll(path.Dir(SSHDropIn), 0o755); err != nil {
		return err
	}
	if err := host.WriteFile(SSHDropIn, []byte(sshDropIn), 0o644); err != nil {
		return err
	}
	undo := func(reason string) error {
		if err := host.Remove(SSHDropIn); err != nil {
			return fmt.Errorf("%s, and taking %s away failed: %w", reason, SSHDropIn, err)
		}
		return Skipped{reason + ": setup left SSH as it was"}
	}
	settings, err := sshdSettings(ctx, host)
	if err != nil {
		return undo("sshd refuses its configuration with " + SSHDropIn + " (" + err.Error() + ")")
	}
	if settings["passwordauthentication"] != "no" || settings["kbdinteractiveauthentication"] == "yes" && settings["usepam"] == "yes" {
		return undo("a setting sshd reads before " + SSHDropIn + " turns passwords on (in /etc/ssh/sshd_config, above its Include)")
	}
	// Debian and Ubuntu call the service ssh, others sshd. A service that
	// does not run (with sshd started for each connection) needs nothing:
	// the next connection reads the settings.
	var failed error
	for _, unit := range []string{"ssh.service", "sshd.service"} {
		_, err := host.Run(ctx, "systemctl", "try-reload-or-restart", unit)
		if err == nil {
			return nil
		}
		failed = errors.Join(failed, err)
	}
	return fmt.Errorf("have sshd read its settings again (systemctl reload ssh): %w", failed)
}
