package v1

import (
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/rand"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

var _ = Describe("RayCluster validating webhook, topology", func() {
	newTopologyRayCluster := func(mappings ...rayv1.TopologyLabelMapping) *rayv1.RayCluster {
		template := corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ray", Image: "rayproject/ray:2.45.0"}}}}
		return &rayv1.RayCluster{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("topo-%d", rand.IntnRange(1000, 9000)), Namespace: "default"},
			Spec: rayv1.RayClusterSpec{
				RayVersion:    "2.45.0",
				HeadGroupSpec: rayv1.HeadGroupSpec{Template: template},
				WorkerGroupSpecs: []rayv1.WorkerGroupSpec{{
					GroupName:   "train",
					MinReplicas: new(int32(1)),
					MaxReplicas: new(int32(1)),
					Template:    template,
					// non-nil so an empty list serializes as [] and hits minItems
					Topology: &rayv1.TopologySpec{LabelMappings: append([]rayv1.TopologyLabelMapping{}, mappings...)},
				}},
			},
		}
	}

	It("accepts allowlisted mappings and rejects the rest", func() {
		accepted := newTopologyRayCluster(rayv1.TopologyLabelMapping{NodeLabel: testAllowedNodeLabels[0], MapTo: "ray.io/zone"})
		Expect(k8sClient.Create(ctx, accepted)).To(Succeed())
		DeferCleanup(k8sClient.Delete, ctx, accepted)

		err := k8sClient.Create(ctx, newTopologyRayCluster(rayv1.TopologyLabelMapping{NodeLabel: "cloud.google.com/gke-nodepool"}))
		Expect(err).To(MatchError(ContainSubstring(`node label "cloud.google.com/gke-nodepool" is not in the operator's allowedNodeLabels`)))
	})

	// CRD schema validation runs before the webhook, so these errors come from the generated minItems and minLength
	It("rejects schema violations", func() {
		err := k8sClient.Create(ctx, newTopologyRayCluster())
		Expect(err).To(MatchError(ContainSubstring("should have at least 1 items")))

		err = k8sClient.Create(ctx, newTopologyRayCluster(rayv1.TopologyLabelMapping{NodeLabel: ""}))
		Expect(err).To(MatchError(ContainSubstring("should be at least 1 chars long")))
	})
})
