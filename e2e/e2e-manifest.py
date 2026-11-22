"""Turn deploy/kill-reason.yaml into the e2e variant (local image, short settle, fixed ClusterIP, control-plane node)."""
import sys

ip = sys.argv[1]
s = sys.stdin.read()
repl = [
    ("image: ghcr.io/jwrede/kill-reason:latest", "image: kill-reason:e2e\n          imagePullPolicy: Never"),
    ("--settle=40s", "--settle=8s"),
    ("      serviceAccountName: kill-reason\n",
     "      serviceAccountName: kill-reason\n"
     "      nodeSelector: {node-role.kubernetes.io/control-plane: \"\"}\n"
     "      tolerations: [{key: node-role.kubernetes.io/control-plane, operator: Exists, effect: NoSchedule}]\n"),
    ("  selector: {app: kill-reason}\n", "  clusterIP: " + ip + "\n  selector: {app: kill-reason}\n"),
]
for old, new in repl:
    if s.count(old) != 1:
        sys.exit(f"deploy manifest changed: {old!r} found {s.count(old)} times")
    s = s.replace(old, new)
sys.stdout.write(s)
