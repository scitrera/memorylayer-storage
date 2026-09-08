// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

//go:build linux

package driver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// k8sPodLauncher runs each domain's mlfs as a Kubernetes "mount pod" — a peer
// pod (NOT a child of the CSI container), pinned to this node, that mounts the
// FUSE at a shared hostPath. Because it is a separate pod with its own cgroup
// scope, a CSI-container restart/upgrade does NOT reap it (the §7.3 Bug 1 fix:
// `setsid` children share the CSI container's cgroup and get cgroup-killed; a
// mount pod does not). The mount propagates to the host via the Bidirectional
// hostPath, so the node plugin bind-mounts it into consumer pods exactly as in
// the process backend. Restart re-adoption is the same state.json + liveness
// path; the persisted Ref is the mount-pod name.
type k8sPodLauncher struct {
	client      kubernetes.Interface
	namespace   string    // where mount pods are created (e.g. kube-system)
	nodeName    string    // pin mount pods to this node
	image       string    // image bundling the mlfs daemon
	domainsBase string    // hostPath base shared with the node plugin + mount pods
	cache       CacheOpts // per-domain cache sizing/placement
	graceSecs   int64     // termination grace for a clean unmount on delete
}

// NewK8sPodLauncher builds the mount-pod backend with an in-cluster client.
func NewK8sPodLauncher(namespace, nodeName, image, domainsBase string, cache CacheOpts) (Launcher, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("in-cluster config: %w", err)
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("k8s client: %w", err)
	}
	if namespace == "" {
		namespace = "kube-system"
	}
	return &k8sPodLauncher{
		client:      client,
		namespace:   namespace,
		nodeName:    nodeName,
		image:       image,
		domainsBase: domainsBase,
		cache:       cache,
		graceSecs:   30,
	}, nil
}

func (l *k8sPodLauncher) Start(ctx context.Context, spec DomainSpec, mountDir, dataDir, credsPath string) (MountHandle, error) {
	// The mount pod reads its scratch + creds from the shared hostPath, so create
	// the dirs node-side first (the pod only mounts, it does not provision).
	cacheDir := l.cache.dirFor(dataDir, spec.Domain)
	for _, d := range []string{dataDir, cacheDir} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return nil, fmt.Errorf("create scratch dir %s: %w", d, err)
		}
	}
	pod := l.podFor(spec, mountDir, dataDir, cacheDir, credsPath)
	_, err := l.client.CoreV1().Pods(l.namespace).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return nil, fmt.Errorf("create mount pod %q: %w", pod.Name, err)
	}
	// AlreadyExists ==> a prior incarnation's pod is still up; adopt it. Readiness
	// (the mount appearing) is polled by the manager via IsMountedLive.
	return &podHandle{client: l.client, namespace: l.namespace, name: pod.Name, grace: l.graceSecs}, nil
}

// Adopt reconstructs a handle for a mount pod created by a prior plugin
// incarnation, from its persisted name (Ref).
func (l *k8sPodLauncher) Adopt(ref, _ string) MountHandle {
	return &podHandle{client: l.client, namespace: l.namespace, name: ref, grace: l.graceSecs}
}

// podFor builds the per-domain mount-pod spec. It runs the SAME mlfs invocation
// as the process backend (remoteMlfsArgs); the meta DSN / NATS creds reach it
// via the args + the shared hostPath creds file (node plugin wrote it 0600),
// never a tenant-visible surface.
func (l *k8sPodLauncher) podFor(spec DomainSpec, mountDir, dataDir, cacheDir, credsPath string) *corev1.Pod {
	privileged := true
	propagation := corev1.MountPropagationBidirectional
	hostDir := corev1.HostPathDirectoryOrCreate
	charDev := corev1.HostPathCharDev
	grace := l.graceSecs

	mounts := []corev1.VolumeMount{
		// The whole domains-base, Bidirectional, so the FUSE mount at mountDir
		// propagates to the host (and thus into consumer pods).
		{Name: "domains", MountPath: l.domainsBase, MountPropagation: &propagation},
		{Name: "fuse", MountPath: "/dev/fuse"},
	}
	volumes := []corev1.Volume{
		{Name: "domains", VolumeSource: corev1.VolumeSource{
			HostPath: &corev1.HostPathVolumeSource{Path: l.domainsBase, Type: &hostDir}}},
		{Name: "fuse", VolumeSource: corev1.VolumeSource{
			HostPath: &corev1.HostPathVolumeSource{Path: "/dev/fuse", Type: &charDev}}},
	}
	// When the cache lives on a separate disk (-cache-base), the mount pod needs
	// that hostPath too so it can write cacheDir (which is rooted under Base).
	if l.cache.Base != "" {
		mounts = append(mounts, corev1.VolumeMount{Name: "cache", MountPath: l.cache.Base})
		volumes = append(volumes, corev1.Volume{Name: "cache", VolumeSource: corev1.VolumeSource{
			HostPath: &corev1.HostPathVolumeSource{Path: l.cache.Base, Type: &hostDir}}})
	}

	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName(l.nodeName, spec.Domain),
			Namespace: l.namespace,
			Labels: map[string]string{
				"app":                          "mlfs-mount",
				"mlfs.csi.scitrera.com/domain": domainLabelValue(spec.Domain),
			},
			// Reserved-prefix storage domains (e.g. "_system") are valid mlfs domain
			// tokens (validateDomain allows '_') but NOT valid k8s LABEL VALUES (which
			// must start/end alphanumeric) — using the raw spec.Domain as a label value
			// made mount-pod creation fail with "Invalid value: \"_system\"". So the
			// label now carries a SANITIZED, kubectl-queryable value (domainLabelValue:
			// a 'd' sentinel prefix for non-alnum-leading tokens, trailing non-alnum
			// trimmed, ≤63), and the ANNOTATION carries the EXACT domain (no charset
			// limit) as the authoritative value. Safe either way: the label is never a
			// selector — Adopt/teardown locate the pod by its hashed NAME (podName).
			Annotations: map[string]string{
				"mlfs.csi.scitrera.com/domain": spec.Domain,
			},
		},
		Spec: corev1.PodSpec{
			NodeName:      l.nodeName, // local mount
			RestartPolicy: corev1.RestartPolicyAlways,
			// The mount pod is pinned (NodeName) to serve PVCs on THIS node, so it
			// must tolerate whatever taints the node carries — otherwise a NoExecute
			// taint (e.g. a GPU/`nvidia.com/gpu` taint, or node-pressure taints) would
			// evict it and wedge every consumer pod's mount. NodeName bypasses the
			// scheduler but NOT the node's NoExecute taint manager. Tolerate-all is
			// correct for a node-local daemon that has to be wherever its consumers are.
			Tolerations:                   []corev1.Toleration{{Operator: corev1.TolerationOpExists}},
			TerminationGracePeriodSeconds: &grace,
			Containers: []corev1.Container{{
				Name:            "mlfs",
				Image:           l.image,
				Command:         []string{"/usr/local/bin/mlfs"},
				Args:            l.cache.appendFlags(remoteMlfsArgs(spec, mountDir, dataDir, cacheDir, credsPath), dataDir),
				SecurityContext: &corev1.SecurityContext{Privileged: &privileged},
				VolumeMounts:    mounts,
			}},
			Volumes: volumes,
		},
	}
}

// podName is a deterministic, DNS-1123-safe mount-pod name unique per
// (node, domain). The domain token and node name can contain characters or
// lengths invalid for a pod name, so it is hashed; the readable domain lives in
// a label.
func podName(nodeName, domain string) string {
	sum := sha256.Sum256([]byte(nodeName + "|" + domain))
	return "mlfs-mount-" + hex.EncodeToString(sum[:12])
}

// domainLabelValue derives a k8s-label-safe, kubectl-queryable value from a domain
// token (the EXACT domain lives in the pod annotation; this is best-effort for
// `-l mlfs.csi.scitrera.com/domain=...` filtering only). k8s label values must be
// ≤63 chars and start/end with [A-Za-z0-9], with [-A-Za-z0-9_.] within — so a
// reserved-prefix domain like "_system" (leading '_') gets a 'd' sentinel →
// "d_system", any trailing non-alnum is trimmed, and it is capped at 63.
func domainLabelValue(domain string) string {
	isAlnum := func(b byte) bool {
		return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9')
	}
	s := domain
	if s == "" {
		return ""
	}
	if !isAlnum(s[0]) {
		s = "d" + s
	}
	if len(s) > 63 {
		s = s[:63]
	}
	for len(s) > 0 && !isAlnum(s[len(s)-1]) {
		s = s[:len(s)-1]
	}
	return s
}

// podHandle stops a mount pod by deleting it (mlfs gets SIGTERM within the
// grace period and unmounts cleanly).
type podHandle struct {
	client    kubernetes.Interface
	namespace string
	name      string
	grace     int64
}

func (h *podHandle) Ref() string { return h.name }

func (h *podHandle) Stop(ctx context.Context) error {
	grace := h.grace
	err := h.client.CoreV1().Pods(h.namespace).Delete(ctx, h.name, metav1.DeleteOptions{
		GracePeriodSeconds: &grace,
	})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete mount pod %q: %w", h.name, err)
	}
	return nil
}
