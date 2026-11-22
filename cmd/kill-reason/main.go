// Command kill-reason records why Kubernetes containers and pods end.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/Jwrede/kill-reason/internal/audit"
	"github.com/Jwrede/kill-reason/internal/controller"
	"github.com/Jwrede/kill-reason/internal/output"
)

var version = "dev"

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "serve":
		err = serve(os.Args[2:])
	case "explain":
		err = explain(os.Args[2:])
	case "version":
		fmt.Println(version)
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "kill-reason:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `usage:
  kill-reason serve   [--listen :8080] [--settle 40s] [--namespace ns] [--audit-token-file f] [--kubeconfig f]
  kill-reason explain [-n ns | -A] [--kubeconfig f] [pod]
  kill-reason version
`)
}

func client(kubeconfig string) (kubernetes.Interface, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeconfig != "" {
		rules.ExplicitPath = kubeconfig
	}
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		return nil, err
	}
	cfg.UserAgent = "kill-reason/" + version
	return kubernetes.NewForConfig(cfg)
}

func serve(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	listen := fs.String("listen", ":8080", "address for /audit, /api/v1/terminations, /metrics, /healthz")
	settle := fs.Duration("settle", 40*time.Second, "wait this long after a termination before classifying it (must exceed --audit-webhook-batch-max-wait)")
	ns := fs.String("namespace", "", "watch only this namespace")
	tokenFile := fs.String("audit-token-file", "", "require this token on /audit (bearer header or /audit/<token>)")
	keep := fs.Duration("audit-retention", 15*time.Minute, "how long to keep audit records")
	kubeconfig := fs.String("kubeconfig", "", "kubeconfig (default: in-cluster or $KUBECONFIG)")
	noEvents := fs.Bool("no-events", false, "do not create Kubernetes Events")
	_ = fs.Parse(args)

	token := ""
	if *tokenFile != "" {
		b, err := os.ReadFile(*tokenFile)
		if err != nil {
			return err
		}
		token = strings.TrimSpace(string(b))
	}
	cs, err := client(*kubeconfig)
	if err != nil {
		return err
	}
	store := audit.NewStore(*keep, token)
	rec := output.NewRecorder(cs, 1000)
	if *noEvents {
		rec.Client = nil
	}
	ctrl := controller.New(cs, store, rec, controller.Options{Settle: *settle, Namespace: *ns})

	mux := http.NewServeMux()
	mux.Handle("/audit", store)
	mux.Handle("/audit/", store)
	mux.HandleFunc("/api/v1/terminations", rec.ServeAPI)
	mux.HandleFunc("/metrics", rec.ServeMetrics(func() map[string]float64 {
		return map[string]float64{"kill_reason_audit_records_received": float64(store.Received())}
	}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	srv := &http.Server{Addr: *listen, Handler: mux, ReadHeaderTimeout: 10 * time.Second}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	errc := make(chan error, 2)
	go func() {
		slog.Info("listening", "addr", *listen, "version", version)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errc <- err
		}
	}()
	go func() { errc <- ctrl.Run(ctx) }()
	select {
	case err = <-errc:
	case <-ctx.Done():
	}
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(sctx)
	return err
}

func explain(args []string) error {
	fs := flag.NewFlagSet("explain", flag.ExitOnError)
	ns := fs.String("n", "", "namespace (default: from kubeconfig)")
	all := fs.Bool("A", false, "all namespaces")
	kubeconfig := fs.String("kubeconfig", "", "kubeconfig")
	_ = fs.Parse(reorder(args))

	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if *kubeconfig != "" {
		rules.ExplicitPath = *kubeconfig
	}
	if *ns == "" && !*all {
		n, _, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{}).Namespace()
		if err == nil {
			*ns = n
		}
	}
	if *all {
		*ns = ""
	}
	cs, err := client(*kubeconfig)
	if err != nil {
		return err
	}
	sel := fields.Set{"source": output.Component}
	if fs.NArg() > 0 {
		sel["involvedObject.name"] = fs.Arg(0)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	list, err := cs.CoreV1().Events(*ns).List(ctx, metav1.ListOptions{FieldSelector: sel.AsSelector().String()})
	if err != nil {
		return err
	}
	items := list.Items
	sort.Slice(items, func(i, j int) bool { return items[i].LastTimestamp.Before(&items[j].LastTimestamp) })
	if len(items) == 0 {
		fmt.Println("no terminations recorded (events expire after the cluster's event TTL, 1h by default)")
		return nil
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "TIME\tNAMESPACE\tPOD\tCAUSE\tDETAIL")
	for _, e := range items {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", e.LastTimestamp.UTC().Format(time.RFC3339), e.Namespace, e.InvolvedObject.Name, e.Reason, e.Message)
	}
	return w.Flush()
}

// reorder moves flags in front of positional arguments so that
// "explain web-1 -n prod" works like kubectl.
func reorder(args []string) []string {
	var flags, pos []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		if strings.HasPrefix(a, "-") {
			flags = append(flags, a)
			if (a == "-n" || a == "--n" || a == "-kubeconfig" || a == "--kubeconfig") && i+1 < len(args) {
				flags = append(flags, args[i+1])
				i++
			}
			continue
		}
		pos = append(pos, a)
	}
	return append(flags, pos...)
}
