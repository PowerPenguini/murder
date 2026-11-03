package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/netip"
	"os"
	"strings"
	"time"

	"golang.org/x/term"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/cluster"
	"github.com/PowerPenguini/murder/internal/setup"
)

const setupUsage = `Usage: sudo murder setup -domain example.com [options]
       sudo murder setup -join <invitation> [options]

Sets this host up as a Murder node: checks Docker, nftables and ZFS,
installs Murder's services, writes /etc/murder/agent.yaml, makes the master
key and the database, starts everything and, with a private network, starts
a cluster of this node. In a terminal it asks for what it cannot find out;
every answer is also an option, for scripts.

With -join, the host joins the cluster an invitation names ("murder cluster
invite" on one of its nodes prints it): it takes the master key, the config
and its tokens from that node, keeping only its own name, addresses and data
choices. -node, -zfs-pool, -cluster-address and -public apply.

Options:
`

// stdin is where setup reads answers; tests replace it.
var stdin io.Reader = os.Stdin

func runSetup(args []string) error {
	flags := flag.NewFlagSet("setup", flag.ContinueOnError)
	flags.Usage = func() {
		fmt.Fprint(os.Stderr, setupUsage)
		flags.PrintDefaults()
	}
	var opts setup.Options
	flags.StringVar(&opts.Domain, "domain", "", "the base domain: tenants live at <tenant>.<domain>")
	flags.StringVar(&opts.Node, "node", "", "this node's name (default: the host name)")
	flags.StringVar(&opts.TLS, "tls", "", "acme (a wildcard certificate through DNS), internal (a local CA, for testing) or off (default: acme with a DNS token, else internal)")
	flags.StringVar(&opts.Email, "email", "", "with -tls acme: for the certificate authority's notices")
	flags.StringVar(&opts.DNSProvider, "dns-provider", "", "hetzner (Hetzner Cloud DNS) or namecheap")
	dnsTokenFile := flags.String("dns-token-file", "", "a file holding the DNS provider's token (Namecheap: user:apikey)")
	hetznerTokenFile := flags.String("hetzner-token-file", "", "a file holding a Hetzner Cloud project token (read and write): powers dead nodes off, and serves as the DNS token with -dns-provider hetzner")
	flags.StringVar(&opts.ZFSPool, "zfs-pool", "", "the ZFS pool for tenants' data (default: the only pool); none for plain directories")
	flags.StringVar(&opts.ClusterAddress, "cluster-address", "", "this node's address on the private network (default: its only private address); none for a node alone")
	var public listFlag
	flags.Var(&public, "public", "a public IP address of this node (default: its own; repeatable)")
	flags.BoolVar(&opts.ManageDNS, "manage-dns", false, "have the cluster keep *.<domain> on the nodes that serve")
	flags.BoolVar(&opts.Force, "force", false, "write the config anew over an existing one (the master key and database are kept)")
	yes := flags.Bool("yes", false, "ask nothing: take the options and what setup finds")
	dryRun := flags.Bool("dry-run", false, "only show what setup would do, and the config it would write")
	join := flags.String("join", "", "join the cluster this invitation names, printed by `murder cluster invite` on one of its nodes")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() > 0 {
		flags.Usage()
		return fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	opts.Public = public
	for file, into := range map[string]*string{*dnsTokenFile: &opts.DNSToken, *hetznerTokenFile: &opts.HetznerToken} {
		if file == "" {
			continue
		}
		data, err := os.ReadFile(file)
		if err != nil {
			return err
		}
		*into = strings.TrimSpace(string(data))
	}

	ctx := context.Background()
	host := setup.System{}
	facts := setup.Inspect(ctx, host)
	if *dryRun {
		// Showing what would be done needs no root.
		facts.Root = true
	}
	interactive := !*yes && term.IsTerminal(int(os.Stdin.Fd()))
	ask := asker{in: bufio.NewReader(stdin), out: stdout}
	if *join != "" {
		return joinCluster(ctx, host, facts, opts, *join, interactive, *dryRun, ask)
	}
	if interactive {
		if err := ask.missing(&opts, facts); err != nil {
			return err
		}
	}
	settings, problems := setup.Settle(opts, facts)
	problems = append(facts.Problems(), problems...)
	if facts.Installed && !opts.Force {
		problems = append(problems, setup.ConfigFile+" exists: this node is set up already (run setup with -force to write the config anew; the master key and database are kept)")
	}
	if err := stopFor(problems); err != nil {
		return err
	}
	describe(settings)
	steps, err := setup.Steps(host, settings.Install(), opts.Force, func() error { return runInit([]string{"-config", setup.ConfigFile}) })
	if err != nil {
		return err
	}
	fmt.Fprintln(stdout, "\nSetup will:")
	for _, step := range steps {
		fmt.Fprintln(stdout, "  - "+step.Say)
	}
	if settings.ClusterAddress != "" {
		fmt.Fprintln(stdout, "  - start a cluster of this node")
	}
	if *dryRun {
		fmt.Fprintf(stdout, "\n%s would be:\n\n%s", setup.ConfigFile, setup.Config(settings))
		return nil
	}
	if interactive && !ask.yes("\nGo on?", true) {
		return errors.New("nothing was changed")
	}
	client, err := install(ctx, steps)
	if err != nil {
		return err
	}
	if settings.ClusterAddress != "" {
		fmt.Fprintln(stdout, "→ start a cluster of this node")
		status, err := client.ClusterStatus(ctx)
		if err != nil || len(status.Members) == 0 {
			if _, err := client.Bootstrap(ctx); err != nil {
				return fmt.Errorf("start the cluster: %w", err)
			}
		}
	}
	nextSteps(settings)
	return nil
}

// stopFor stops setup, before it changes anything, for problems.
func stopFor(problems []string) error {
	if len(problems) == 0 {
		return nil
	}
	fmt.Fprintln(stdout, "Murder cannot be set up here yet:")
	for _, problem := range problems {
		fmt.Fprintln(stdout, "  - "+problem)
	}
	return errors.New("setup stopped; nothing was changed")
}

// install takes the steps and waits for the agent to answer.
func install(ctx context.Context, steps []setup.Step) (*api.Client, error) {
	fmt.Fprintln(stdout)
	for _, step := range steps {
		fmt.Fprintln(stdout, "→ "+step.Say)
		if err := step.Do(ctx); err != nil {
			return nil, fmt.Errorf("%s: %w", step.Say, err)
		}
	}
	client := api.NewClient(defaultSocket())
	if err := waitForAgent(ctx, client, time.Minute); err != nil {
		return nil, fmt.Errorf("%w (journalctl -u murder-agent says why)", err)
	}
	return client, nil
}

// joinCluster sets the host up as a node of the cluster an invitation
// names. The invitation is used only once the host is ready for it.
func joinCluster(ctx context.Context, host setup.System, facts setup.Facts, opts setup.Options, text string, interactive, dryRun bool, ask asker) error {
	invite, err := cluster.ParseInvite(text)
	if err != nil {
		return err
	}
	peer := netip.MustParseAddrPort(invite.Address)
	own, problems := setup.SettleOwn(opts, facts, peer)
	problems = append(facts.Problems(), problems...)
	if facts.Installed && !opts.Force {
		problems = append(problems, setup.ConfigFile+" exists: this host is set up already")
	}
	if err := stopFor(problems); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "\nNode %s joins the cluster of %s\n", own.Node, invite.Address)
	if own.ZFSPool != "" {
		fmt.Fprintf(stdout, "  data:      a ZFS dataset per tenant, in %s/murder/tenants\n", own.ZFSPool)
	} else {
		fmt.Fprintln(stdout, "  data:      plain directories (no disk limits, no copies on other nodes)")
	}
	fmt.Fprintf(stdout, "  cluster:   at %s on the private network\n", own.ClusterAddress)
	if len(own.Public) > 0 {
		fmt.Fprintf(stdout, "  public:    %s\n", strings.Join(own.Public, ", "))
	}
	if dryRun {
		fmt.Fprintln(stdout, "\nThe invitation was not used: without -dry-run, setup takes the cluster's key, config and tokens with it.")
		return nil
	}
	if interactive && !ask.yes("\nGo on?", true) {
		return errors.New("nothing was changed; the invitation was not used")
	}
	fmt.Fprintln(stdout, "\n→ take the cluster's master key, config and tokens from "+invite.Address)
	asking, cancel := context.WithTimeout(ctx, time.Minute)
	bundle, err := cluster.Join(asking, invite, own.Node)
	cancel()
	if err != nil {
		return err
	}
	in, agent, err := setup.Adopt(bundle, own)
	if err != nil {
		return err
	}
	steps, err := setup.Steps(host, in, opts.Force, func() error { return runInit([]string{"-config", setup.ConfigFile}) })
	if err != nil {
		return err
	}
	client, err := install(ctx, steps)
	if err != nil {
		return err
	}
	fmt.Fprintln(stdout, "→ join the cluster "+bundle.Cluster)
	joining, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	status, err := client.Join(joining, invite.Address)
	if err != nil {
		return fmt.Errorf("join the cluster: %w", err)
	}
	fmt.Fprintf(stdout, "\n%s is a node of cluster %s, with %d nodes.\n", own.Node, bundle.Cluster, len(status.Members))
	fmt.Fprintln(stdout, "\nLeft to do:")
	if agent.Cluster.DNS == nil && len(own.Public) > 0 {
		fmt.Fprintf(stdout, "  - add %s to the A records of %s and *.%s\n", strings.Join(own.Public, ", "), agent.BaseDomain, agent.BaseDomain)
	}
	ports := "80 and 443 (TCP)"
	if in.Panel {
		ports += ", and 51820 (UDP) for the panel's tunnel"
	}
	fmt.Fprintf(stdout, "  - open %s in the provider's firewall; keep %d and %d to the private network\n", ports, peer.Port(), agent.Edge.RelayPort)
	return nil
}

// defaultSocket is the agent's socket as the CLI finds it.
func defaultSocket() string {
	flags := flag.NewFlagSet("", flag.ContinueOnError)
	return *socketPath(flags)
}

// waitForAgent waits until the agent answers on its socket.
func waitForAgent(ctx context.Context, client *api.Client, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		ask, cancel := context.WithTimeout(ctx, 2*time.Second)
		_, err := client.Apps(ask)
		cancel()
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("the agent did not answer within %s: %w", timeout, err)
		}
		time.Sleep(time.Second)
	}
}

// describe says what setup settled on, tokens left out.
func describe(s setup.Settings) {
	fmt.Fprintf(stdout, "\nNode %s, tenants at <tenant>.%s\n", s.Node, s.Domain)
	if s.ZFSPool != "" {
		fmt.Fprintf(stdout, "  data:      a ZFS dataset per tenant, in %s/murder/tenants\n", s.ZFSPool)
	} else {
		fmt.Fprintln(stdout, "  data:      plain directories (no disk limits, no copies on other nodes)")
	}
	switch s.TLS {
	case "acme":
		fmt.Fprintf(stdout, "  TLS:       a wildcard certificate from Let's Encrypt, through %s DNS\n", s.DNSProvider)
	case "internal":
		fmt.Fprintln(stdout, "  TLS:       a local CA browsers do not trust (for testing)")
	default:
		fmt.Fprintln(stdout, "  TLS:       off, plain HTTP")
	}
	if s.ClusterAddress != "" {
		fmt.Fprintf(stdout, "  cluster:   at %s on the private network\n", s.ClusterAddress)
	} else {
		fmt.Fprintln(stdout, "  cluster:   none, a node alone")
	}
	if len(s.Public) > 0 {
		fmt.Fprintf(stdout, "  public:    %s\n", strings.Join(s.Public, ", "))
	}
	if s.Failover {
		fmt.Fprintln(stdout, "  failover:  a dead node is powered off through Hetzner Cloud, its tenants run from their copies")
	}
	if s.ManageDNS {
		fmt.Fprintf(stdout, "  DNS:       the cluster keeps *.%s on the nodes that serve\n", s.Domain)
	}
	if s.Panel {
		fmt.Fprintf(stdout, "  panel:     https://panel.%s inside a WireGuard tunnel\n", s.Domain)
	}
}

// nextSteps says what is left to do by hand.
func nextSteps(s setup.Settings) {
	fmt.Fprintf(stdout, "\nMurder runs on %s. Left to do:\n", s.Node)
	n := 0
	step := func(format string, args ...any) {
		n++
		fmt.Fprintf(stdout, "  %d. "+format+"\n", append([]any{n}, args...)...)
	}
	fmt.Fprintf(stdout, "\nKeep a copy of %s offline, in a password manager: without it the\ndatabase's secrets cannot be read.\n\n", "/etc/murder/master.key")
	if !s.ManageDNS && len(s.Public) > 0 {
		step("point %s and *.%s at %s (A records)", s.Domain, s.Domain, strings.Join(s.Public, ", "))
	}
	ports := "80 and 443 (TCP)"
	if s.Panel {
		ports += ", and 51820 (UDP) for the panel's tunnel"
	}
	step("open %s in the provider's firewall; keep %d and 7402 to the private network", ports, setup.ClusterPort)
	if s.Panel {
		step("reach the panel: `murder admin device add laptop` (a QR code for the WireGuard app), then `murder admin invite <your name>`")
	}
	step("run an app: write its spec, `murder app apply web.yaml`, `murder tenant create acme -app web` (docs/apps.md)")
	if s.ZFSPool == "" {
		fmt.Fprintln(stdout, "\nWith plain directories, tenants have no disk limits and no copies on other nodes.")
	}
}

// asker asks in a terminal for what the options left out.
type asker struct {
	in  *bufio.Reader
	out io.Writer
}

func (a asker) line(prompt, fallback string) string {
	if fallback != "" {
		fmt.Fprintf(a.out, "%s [%s]: ", prompt, fallback)
	} else {
		fmt.Fprintf(a.out, "%s: ", prompt)
	}
	text, _ := a.in.ReadString('\n')
	if text = strings.TrimSpace(text); text == "" {
		return fallback
	}
	return text
}

// secret reads without echo where it can.
func (a asker) secret(prompt string) string {
	fmt.Fprintf(a.out, "%s (not shown, empty for none): ", prompt)
	if file, ok := stdin.(*os.File); ok && term.IsTerminal(int(file.Fd())) {
		data, _ := term.ReadPassword(int(file.Fd()))
		fmt.Fprintln(a.out)
		return strings.TrimSpace(string(data))
	}
	text, _ := a.in.ReadString('\n')
	return strings.TrimSpace(text)
}

func (a asker) yes(prompt string, fallback bool) bool {
	hint := "Y/n"
	if !fallback {
		hint = "y/N"
	}
	answer := strings.ToLower(a.line(prompt+" "+hint, ""))
	if answer == "" {
		return fallback
	}
	return strings.HasPrefix(answer, "y")
}

func (a asker) missing(opts *setup.Options, facts setup.Facts) error {
	if opts.Domain == "" {
		opts.Domain = a.line("Base domain (tenants live at <tenant>.<domain>)", "")
	}
	if opts.TLS == "" && opts.DNSProvider == "" {
		switch a.line("Certificates: [1] Let's Encrypt through Hetzner DNS, [2] through Namecheap DNS, [3] a local CA for testing, [4] none", "1") {
		case "1":
			opts.TLS, opts.DNSProvider = "acme", "hetzner"
		case "2":
			opts.TLS, opts.DNSProvider = "acme", "namecheap"
		case "3":
			opts.TLS = "internal"
		case "4":
			opts.TLS = "off"
		}
	}
	if opts.DNSProvider == "hetzner" && opts.DNSToken == "" && opts.HetznerToken == "" {
		opts.HetznerToken = a.secret("Hetzner Cloud project token (read and write: DNS, and powering dead nodes off)")
	}
	if opts.DNSProvider == "namecheap" && opts.DNSToken == "" {
		opts.DNSToken = a.secret("Namecheap API user and key, as user:apikey")
	}
	if opts.TLS == "acme" && opts.Email == "" {
		opts.Email = a.line("E-mail for Let's Encrypt's notices", "")
	}
	if opts.ZFSPool == "" && len(facts.Pools) > 1 {
		opts.ZFSPool = a.line("ZFS pool for tenants' data ("+strings.Join(facts.Pools, ", ")+")", facts.Pools[0])
	}
	if opts.ClusterAddress == "" && len(facts.Private) > 1 {
		opts.ClusterAddress = a.line("This node's address on the private network", facts.Private[0].String())
	}
	if opts.DNSProvider != "" && !opts.ManageDNS && len(facts.Private) > 0 {
		opts.ManageDNS = a.yes("Have the cluster keep *."+opts.Domain+"'s DNS records on the nodes that serve?", true)
	}
	return nil
}
