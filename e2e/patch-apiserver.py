"""Add audit webhook flags and the audit config mount to a kubeadm kube-apiserver manifest."""
import sys

path = sys.argv[1]
s = open(path).read()
flags = (
    "    - kube-apiserver\n"
    "    - --audit-policy-file=/etc/kubernetes/audit/policy.yaml\n"
    "    - --audit-webhook-config-file=/etc/kubernetes/audit/webhook.yaml\n"
    "    - --audit-webhook-batch-max-wait=1s\n"
)
mount = "    volumeMounts:\n    - mountPath: /etc/kubernetes/audit\n      name: kill-reason-audit\n      readOnly: true\n"
volume = "  volumes:\n  - hostPath:\n      path: /etc/kubernetes/audit\n      type: Directory\n    name: kill-reason-audit\n"
for old, new in (("    - kube-apiserver\n", flags), ("    volumeMounts:\n", mount), ("  volumes:\n", volume)):
    if s.count(old) != 1:
        sys.exit(f"unexpected manifest layout: {old!r} found {s.count(old)} times")
    s = s.replace(old, new)
open(path, "w").write(s)
