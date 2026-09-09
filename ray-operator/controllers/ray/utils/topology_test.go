package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

var (
	zoneMapping   = rayv1.TopologyLabelMapping{NodeLabel: "topology.kubernetes.io/zone", MapTo: "ray.io/zone"}
	cliqueMapping = rayv1.TopologyLabelMapping{NodeLabel: "nvidia.com/gpu.clique"}
)

func topologyWorkerGroup(mappings ...rayv1.TopologyLabelMapping) rayv1.WorkerGroupSpec {
	return rayv1.WorkerGroupSpec{
		GroupName:   "train",
		MinReplicas: new(int32(1)),
		MaxReplicas: new(int32(1)),
		Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{
			{Name: "ray-worker", Image: "rayproject/ray"},
		}}},
		Topology: &rayv1.TopologySpec{LabelMappings: mappings},
	}
}

func TestValidateWorkerGroupTopology(t *testing.T) {
	tests := []struct {
		mutate        func(group *rayv1.WorkerGroupSpec)
		name          string
		webhooks      string
		rayVersion    string
		errorContains string
	}{
		{name: "valid mappings", webhooks: "true", rayVersion: "2.45.0"},
		{name: "unset rayVersion is accepted", webhooks: "true"},
		{name: "no topology is a no-op even without webhooks", mutate: func(g *rayv1.WorkerGroupSpec) { g.Topology = nil }},
		{name: "webhooks disabled", webhooks: "false", errorContains: "requires the KubeRay operator to run with ENABLE_WEBHOOKS=true"},
		{name: "rayVersion too old", webhooks: "true", rayVersion: "2.44.0", errorContains: "minimum Ray version is 2.45.0, got 2.44.0"},
		{name: "rayVersion malformed", webhooks: "true", rayVersion: "not-a-version", errorContains: "rayVersion format is invalid"},
		{
			name: "empty labelMappings", webhooks: "true",
			mutate:        func(g *rayv1.WorkerGroupSpec) { g.Topology.LabelMappings = nil },
			errorContains: "empty labelMappings list",
		},
		{
			name: "overwrite-container-cmd annotation", webhooks: "true",
			mutate: func(g *rayv1.WorkerGroupSpec) {
				g.Template.Annotations = map[string]string{RayOverwriteContainerCmdAnnotationKey: "true"}
			},
			errorContains: RayOverwriteContainerCmdAnnotationKey,
		},
		{
			name: "user command runs ray start", webhooks: "true",
			mutate: func(g *rayv1.WorkerGroupSpec) {
				g.Template.Spec.Containers[0].Command = []string{"ray start --address=head:6379 --block"}
			},
			errorContains: "runs ray start itself",
		},
		{
			name: "invalid nodeLabel key", webhooks: "true",
			mutate: func(g *rayv1.WorkerGroupSpec) {
				g.Topology.LabelMappings = []rayv1.TopologyLabelMapping{{NodeLabel: "not a label!"}}
			},
			errorContains: `nodeLabel "not a label!" is not a valid label key`,
		},
		{
			name: "invalid mapTo key", webhooks: "true",
			mutate: func(g *rayv1.WorkerGroupSpec) {
				g.Topology.LabelMappings = []rayv1.TopologyLabelMapping{{NodeLabel: "topology.kubernetes.io/zone", MapTo: "ray.io/"}}
			},
			errorContains: `mapTo "ray.io/" is not a valid label key`,
		},
		{
			name: "two mappings deliver the same Ray key", webhooks: "true",
			mutate: func(g *rayv1.WorkerGroupSpec) {
				g.Topology.LabelMappings = []rayv1.TopologyLabelMapping{
					{NodeLabel: "nvidia.com/gpu.clique", MapTo: "ray.io/accelerator-domain"},
					{NodeLabel: "cloud.google.com/gce-topology-block", MapTo: "ray.io/accelerator-domain"},
				}
			},
			errorContains: `deliver Ray label "ray.io/accelerator-domain" twice`,
		},
		{
			name: "Ray key collides with static labels", webhooks: "true",
			mutate: func(g *rayv1.WorkerGroupSpec) {
				g.Labels = map[string]string{"ray.io/zone": "us-central1-a"}
			},
			errorContains: `sets Ray label "ray.io/zone" both in labels and in topology.labelMappings`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("ENABLE_WEBHOOKS", tt.webhooks)
			group := topologyWorkerGroup(zoneMapping, cliqueMapping)
			if tt.mutate != nil {
				tt.mutate(&group)
			}
			err := ValidateWorkerGroupTopology(&group, tt.rayVersion)
			if tt.errorContains == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.errorContains)
		})
	}
}

// TestValidateRayClusterSpec_Topology checks that ValidateRayClusterSpec applies the topology rules to worker groups
func TestValidateRayClusterSpec_Topology(t *testing.T) {
	spec := createBasicRayClusterSpec()
	spec.RayVersion = "2.45.0"
	spec.WorkerGroupSpecs = []rayv1.WorkerGroupSpec{topologyWorkerGroup(zoneMapping)}

	t.Setenv("ENABLE_WEBHOOKS", "true")
	require.NoError(t, ValidateRayClusterSpec(spec, nil))

	t.Setenv("ENABLE_WEBHOOKS", "")
	require.ErrorContains(t, ValidateRayClusterSpec(spec, nil), "requires the KubeRay operator to run with ENABLE_WEBHOOKS=true")
}

func TestBuildTopologyLabels(t *testing.T) {
	allowed := sets.New(zoneMapping.NodeLabel, cliqueMapping.NodeLabel)
	mappings := []rayv1.TopologyLabelMapping{zoneMapping, cliqueMapping}

	// one mapping renames, the other keeps the node label key
	labels, err := BuildTopologyLabels(map[string]string{zoneMapping.NodeLabel: "us-central1-a", cliqueMapping.NodeLabel: "abc.0", "unrelated": "x"}, mappings, allowed)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"ray.io/zone": "us-central1-a", cliqueMapping.NodeLabel: "abc.0"}, labels)

	failures := map[string]struct {
		nodeLabels    map[string]string
		allowed       sets.Set[string]
		errorContains string
	}{
		"missing label":   {map[string]string{zoneMapping.NodeLabel: "us-central1-a"}, allowed, `node lacks label "nvidia.com/gpu.clique"`},
		"empty value":     {map[string]string{zoneMapping.NodeLabel: "", cliqueMapping.NodeLabel: "abc.0"}, allowed, `node label "topology.kubernetes.io/zone" is empty`},
		"not allowlisted": {map[string]string{zoneMapping.NodeLabel: "us-central1-a", cliqueMapping.NodeLabel: "abc.0"}, sets.New(zoneMapping.NodeLabel), `node label "nvidia.com/gpu.clique" is not in the operator's allowedNodeLabels`},
	}
	for name, tc := range failures {
		t.Run(name, func(t *testing.T) {
			_, err := BuildTopologyLabels(tc.nodeLabels, mappings, tc.allowed)
			require.ErrorContains(t, err, tc.errorContains)
		})
	}
}
