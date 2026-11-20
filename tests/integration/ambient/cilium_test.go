//go:build integ

// Copyright Istio Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ambient

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"istio.io/api/label"
	"istio.io/istio/pkg/kube"
	"istio.io/istio/pkg/test/framework"
	"istio.io/istio/pkg/test/framework/components/echo"
	"istio.io/istio/pkg/test/framework/components/echo/check"
	"istio.io/istio/pkg/test/framework/components/echo/common/ports"
	"istio.io/istio/pkg/test/framework/components/echo/deployment"
	"istio.io/istio/pkg/test/framework/components/namespace"
	"istio.io/istio/pkg/test/util/assert"
	"istio.io/istio/pkg/test/util/retry"
)

func TestCiliumHostProbes(t *testing.T) {
	if os.Getenv("KUBERNETES_CNI") != "cilium" {
		t.Skip("requires the Cilium BPF host-routing CI environment")
	}
	framework.NewTest(t).Run(func(t framework.TestContext) {
		if len(t.Clusters()) != 1 {
			t.Skip("requires the dedicated single-cluster Cilium configuration")
		}
		ns := namespace.NewOrFail(t, namespace.Config{
			Prefix: "cilium-probes",
			Labels: map[string]string{label.IoIstioDataplaneMode.Name: "ambient"},
		})
		t.ConfigIstio().YAML(ns.Name(), `
apiVersion: security.istio.io/v1
kind: PeerAuthentication
metadata:
  name: strict
spec:
  mtls:
    mode: STRICT
---
apiVersion: security.istio.io/v1
kind: AuthorizationPolicy
metadata:
  name: authenticated
spec:
  rules:
  - from:
    - source:
        principals: ["*"]
`).ApplyOrFail(t)

		nakedNS := namespace.NewOrFail(t, namespace.Config{Prefix: "cilium-naked"})
		var server, client, naked echo.Instance
		deployment.New(t).
			With(&server, echo.Config{
				Namespace: ns, Service: "probe-server", Ports: []echo.Port{ports.HTTP},
			}).
			With(&client, echo.Config{
				Namespace: ns, Service: "probe-client", Ports: []echo.Port{ports.HTTP},
			}).
			With(&naked, echo.Config{
				Namespace: nakedNS, Service: "naked", Ports: []echo.Port{ports.HTTP},
			}).BuildOrFail(t)
		c := t.Clusters().Default()
		verifyCiliumProbeDatapath(t, c)
		checkTraffic := func() {
			client.CallOrFail(t, echo.CallOptions{
				To: server, Port: echo.Port{Name: "http"}, Count: 3, NewConnectionPerRequest: true, Check: check.OK(),
			})
			naked.CallOrFail(t, echo.CallOptions{
				To: server, Port: echo.Port{Name: "http"}, Count: 1, NewConnectionPerRequest: true, Check: check.NotOK(),
			})
		}
		checkTraffic()

		deployments, err := c.Kube().AppsV1().Deployments(ns.Name()).List(context.Background(), metav1.ListOptions{})
		assert.NoError(t, err)
		var template *corev1.PodTemplateSpec
		for _, d := range deployments.Items {
			if d.Spec.Template.Labels["app"] == "probe-server" {
				for index := range d.Spec.Template.Spec.Containers {
					container := &d.Spec.Template.Spec.Containers[index]
					container.LivenessProbe = &corev1.Probe{
						ProbeHandler: corev1.ProbeHandler{
							HTTPGet: &corev1.HTTPGetAction{Path: "/", Port: intstr.FromInt32(8080)},
						},
						PeriodSeconds: 2, FailureThreshold: 3,
					}
				}
				_, err := c.Kube().AppsV1().Deployments(ns.Name()).Update(context.Background(), &d, metav1.UpdateOptions{})
				assert.NoError(t, err)
				template = d.Spec.Template.DeepCopy()
			}
		}
		if template == nil {
			t.Fatal("probe-server deployment not found")
		}
		assert.NoError(t, server.Restart())
		checkTraffic()
		start := time.Now()
		retry.UntilSuccessOrFail(t, func() error {
			pods, err := c.Kube().CoreV1().Pods(ns.Name()).List(context.Background(), metav1.ListOptions{LabelSelector: "app=probe-server"})
			if err != nil {
				return err
			}
			if len(pods.Items) != 1 {
				return fmt.Errorf("waiting for one probe-server pod, got %d", len(pods.Items))
			}
			for _, status := range pods.Items[0].Status.ContainerStatuses {
				if status.RestartCount != 0 {
					t.Fatalf("healthy HTTP liveness/TCP startup probes restarted %s", status.Name)
				}
				if !status.Ready || status.Started == nil || !*status.Started {
					return fmt.Errorf("container %s is not ready and started", status.Name)
				}
			}
			if len(pods.Items[0].Status.ContainerStatuses) == 0 || time.Since(start) < 8*time.Second {
				return fmt.Errorf("observing healthy HTTP liveness probes")
			}
			return nil
		}, retry.Timeout(time.Minute), retry.Delay(time.Second))
		for _, tc := range []struct {
			name, path, message string
			closedPort          bool
		}{
			{name: "http-503", path: "/?codes=503", message: "503"},
			{name: "closed-port", closedPort: true, message: "connection refused"},
		} {
			t.NewSubTest(tc.name).Run(func(t framework.TestContext) {
				pod := &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{Name: tc.name, Namespace: ns.Name(), Labels: map[string]string{"app": tc.name}},
					Spec:       *template.Spec.DeepCopy(),
				}
				for index := range pod.Spec.Containers {
					container := &pod.Spec.Containers[index]
					container.ReadinessProbe = &corev1.Probe{
						ProbeHandler: corev1.ProbeHandler{
							HTTPGet: &corev1.HTTPGetAction{Path: tc.path, Port: intstr.FromInt32(8080)},
						},
						PeriodSeconds: 1, FailureThreshold: 1,
					}
					if tc.closedPort {
						container.ReadinessProbe.ProbeHandler = corev1.ProbeHandler{
							TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(1)},
						}
					}
				}
				_, err := c.Kube().CoreV1().Pods(ns.Name()).Create(context.Background(), pod, metav1.CreateOptions{})
				assert.NoError(t, err)
				retry.UntilSuccessOrFail(t, func() error {
					current, err := c.Kube().CoreV1().Pods(ns.Name()).Get(context.Background(), pod.Name, metav1.GetOptions{})
					if err != nil {
						return err
					}
					if current.Annotations["ambient.istio.io/redirection"] != "enabled" {
						return fmt.Errorf("pod is not yet enrolled in ambient")
					}
					for _, condition := range current.Status.Conditions {
						if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
							return fmt.Errorf("unhealthy control unexpectedly Ready")
						}
					}
					events, err := c.Kube().CoreV1().Events(ns.Name()).List(context.Background(),
						metav1.ListOptions{FieldSelector: "involvedObject.uid=" + string(current.UID)})
					if err != nil {
						return err
					}
					for _, event := range events.Items {
						if event.Reason == "Unhealthy" && strings.Contains(event.Message, tc.message) {
							return nil
						}
					}
					return fmt.Errorf("waiting for kubelet probe failure containing %q", tc.message)
				}, retry.Timeout(2*time.Minute), retry.Delay(time.Second))
			})
		}

		systemNS := i.Settings().SystemNamespace
		ds := GetCNIDaemonSet(t, c, systemNS)
		original := ds.DeepCopy()
		t.Cleanup(func() {
			current := GetCNIDaemonSet(t, c, systemNS)
			current.Spec.Template = original.Spec.Template
			_, err := c.Kube().AppsV1().DaemonSets(systemNS).Update(context.Background(), current, metav1.UpdateOptions{})
			assert.NoError(t, err)
			waitForCiliumCNI(t, c, systemNS)
		})
		for _, phase := range []string{"restart", "disable", "enable"} {
			ds = GetCNIDaemonSet(t, c, systemNS)
			if ds.Spec.Template.Annotations == nil {
				ds.Spec.Template.Annotations = map[string]string{}
			}
			ds.Spec.Template.Annotations["test.istio.io/cilium-phase"] = phase
			if phase != "restart" {
				value := "false"
				if phase == "enable" {
					value = "true"
				}
				found := false
				for index := range ds.Spec.Template.Spec.Containers {
					container := &ds.Spec.Template.Spec.Containers[index]
					if container.Name == "install-cni" {
						found = true
						replaced := false
						for j := range container.Env {
							if container.Env[j].Name == "AMBIENT_ENABLE_HOST_PROBE_LOCAL_ROUTE" {
								container.Env[j].Value, container.Env[j].ValueFrom = value, nil
								replaced = true
							}
						}
						if !replaced {
							container.Env = append(container.Env, corev1.EnvVar{Name: "AMBIENT_ENABLE_HOST_PROBE_LOCAL_ROUTE", Value: value})
						}
					}
				}
				if !found {
					t.Fatal("install-cni container not found")
				}
			}
			_, err := c.Kube().AppsV1().DaemonSets(systemNS).Update(context.Background(), ds, metav1.UpdateOptions{})
			assert.NoError(t, err)
			waitForCiliumCNI(t, c, systemNS)
			verifyCiliumProbeRoutes(t, c, phase != "disable")
			if phase != "disable" {
				assert.NoError(t, server.Restart())
				checkTraffic()
			}
		}
	})
}

func waitForCiliumCNI(t framework.TestContext, c kube.CLIClient, systemNS string) {
	t.Helper()
	retry.UntilSuccessOrFail(t, func() error {
		ds, err := c.Kube().AppsV1().DaemonSets(systemNS).Get(context.Background(), "istio-cni-node", metav1.GetOptions{})
		if err != nil {
			return err
		}
		if ds.Status.ObservedGeneration < ds.Generation || ds.Status.DesiredNumberScheduled == 0 ||
			ds.Status.UpdatedNumberScheduled != ds.Status.DesiredNumberScheduled ||
			ds.Status.NumberReady != ds.Status.DesiredNumberScheduled || ds.Status.NumberUnavailable != 0 {
			return fmt.Errorf("waiting for CNI rollout: %+v", ds.Status)
		}
		return nil
	}, retry.Timeout(5*time.Minute), retry.Delay(2*time.Second))
}

func verifyCiliumProbeDatapath(t framework.TestContext, c kube.CLIClient) {
	t.Helper()
	pods, err := ciliumProbeAgents(c)
	assert.NoError(t, err)
	for _, pod := range pods {
		out, stderr, err := c.PodExec(pod.Name, pod.Namespace, "cilium-agent", "cilium-dbg status --verbose")
		if err != nil {
			t.Fatalf("read Cilium status on %s: %v: %s", pod.Spec.NodeName, err, stderr)
		}
		for _, pattern := range []string{`Routing:\s+Network:\s+Native\s+Host:\s+BPF`, `Masquerading:\s+BPF`} {
			if !regexp.MustCompile(pattern).MatchString(out) {
				t.Fatalf("required Cilium BPF configuration missing on %s:\n%s", pod.Spec.NodeName, out)
			}
		}
		// The host endpoint has a separate datapath; inspect the pod-facing programs.
		_, stderr, err = c.PodExecCommands(pod.Name, pod.Namespace, "cilium-agent", []string{
			"sh", "-c",
			`set -eu; seen=0; for h in /var/run/cilium/state/*/ep_config.h; do test -f "$h"; ` +
				`if grep -q "^#define HOST_ENDPOINT " "$h"; then continue; fi; seen=$((seen + 1)); ` +
				`if grep -q "^#define ENABLE_SKIP_FIB" "$h"; then echo "FIB lookup disabled: $h" >&2; exit 1; fi; done; test "$seen" -gt 0`,
		})
		if err != nil {
			t.Fatalf("check Cilium FIB configuration on %s: %v: %s", pod.Spec.NodeName, err, stderr)
		}
	}
	verifyCiliumProbeRoutes(t, c, true)
}

func verifyCiliumProbeRoutes(t framework.TestContext, c kube.CLIClient, enabled bool) {
	t.Helper()
	retry.UntilSuccessOrFail(t, func() error {
		pods, err := ciliumProbeAgents(c)
		if err != nil {
			return err
		}
		for _, pod := range pods {
			out, stderr, err := c.PodExec(pod.Name, pod.Namespace, "cilium-agent", "ip -4 route show table local proto 196")
			if err != nil {
				return fmt.Errorf("list probe routes: %w: %s", err, stderr)
			}
			found := strings.Contains(out, "local 169.254.7.127 dev lo")
			if found != enabled {
				return fmt.Errorf("probe route on %s: got %q, enabled=%v", pod.Spec.NodeName, out, enabled)
			}
		}
		return nil
	}, retry.Timeout(time.Minute), retry.Delay(time.Second))
}

func ciliumProbeAgents(c kube.CLIClient) ([]corev1.Pod, error) {
	pods, err := c.Kube().CoreV1().Pods("kube-system").List(context.Background(), metav1.ListOptions{LabelSelector: "k8s-app=cilium"})
	if err != nil {
		return nil, err
	}
	nodes, err := c.Kube().CoreV1().Nodes().List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	if len(nodes.Items) < 2 {
		return nil, fmt.Errorf("Cilium regression requires at least two nodes")
	}
	for _, node := range nodes.Items {
		found := false
		for _, pod := range pods.Items {
			if pod.Spec.NodeName == node.Name && pod.Spec.HostNetwork && pod.DeletionTimestamp == nil {
				found = true
			}
		}
		if !found {
			return nil, fmt.Errorf("no host-network Cilium agent on %s", node.Name)
		}
	}
	return pods.Items, nil
}
