package utils

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/version"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

// TopologyRayLabelKey returns the Ray label key a topology mapping delivers its value under.
func TopologyRayLabelKey(m rayv1.TopologyLabelMapping) string {
	if m.MapTo != "" {
		return m.MapTo
	}
	return m.NodeLabel
}

// ValidateWorkerGroupTopology validates a worker group's topology field. The operator allowlist is checked
// by the RayCluster webhook instead
func ValidateWorkerGroupTopology(workerGroup *rayv1.WorkerGroupSpec, rayVersion string) error {
	if workerGroup.Topology == nil {
		return nil
	}
	groupName := workerGroup.GroupName

	if strings.ToLower(os.Getenv("ENABLE_WEBHOOKS")) != "true" {
		return fmt.Errorf("worker group %s sets topology, which requires the KubeRay operator to run with ENABLE_WEBHOOKS=true", groupName)
	}

	if rayVersion != "" {
		parsedVersion, err := version.ParseGeneric(rayVersion)
		if err != nil {
			return fmt.Errorf("worker group %s sets topology but rayVersion format is invalid: %s, %w", groupName, rayVersion, err)
		}
		if parsedVersion.LessThan(version.MustParseGeneric(MinRayVersionForTopologyLabels)) {
			return fmt.Errorf("worker group %s sets topology but minimum Ray version is %s, got %s", groupName, MinRayVersionForTopologyLabels, rayVersion)
		}
	}

	if len(workerGroup.Topology.LabelMappings) == 0 {
		return fmt.Errorf("worker group %s sets topology with an empty labelMappings list", groupName)
	}

	if v, ok := workerGroup.Template.Annotations[RayOverwriteContainerCmdAnnotationKey]; ok && strings.ToLower(v) == "true" {
		return fmt.Errorf("worker group %s sets both topology and the %s annotation; node label delivery needs the KubeRay-generated ray start command", groupName, RayOverwriteContainerCmdAnnotationKey)
	}
	if len(workerGroup.Template.Spec.Containers) > RayContainerIndex {
		container := workerGroup.Template.Spec.Containers[RayContainerIndex]
		userCmd := strings.Join(append(append([]string{}, container.Command...), container.Args...), " ")
		if strings.Contains(userCmd, "ray start") {
			return fmt.Errorf("worker group %s sets topology but its container command runs ray start itself; node label delivery needs the KubeRay-generated ray start command", groupName)
		}
	}

	deliveredBy := make(map[string]string, len(workerGroup.Topology.LabelMappings))
	for i, mapping := range workerGroup.Topology.LabelMappings {
		if errs := validation.IsQualifiedName(mapping.NodeLabel); len(errs) > 0 {
			return fmt.Errorf("worker group %s topology.labelMappings[%d].nodeLabel %q is not a valid label key: %s", groupName, i, mapping.NodeLabel, strings.Join(errs, "; "))
		}
		if mapping.MapTo != "" {
			if errs := validation.IsQualifiedName(mapping.MapTo); len(errs) > 0 {
				return fmt.Errorf("worker group %s topology.labelMappings[%d].mapTo %q is not a valid label key: %s", groupName, i, mapping.MapTo, strings.Join(errs, "; "))
			}
		}
		rayKey := TopologyRayLabelKey(mapping)
		if previous, dup := deliveredBy[rayKey]; dup {
			return fmt.Errorf("worker group %s topology.labelMappings deliver Ray label %q twice (from node labels %q and %q)", groupName, rayKey, previous, mapping.NodeLabel)
		}
		deliveredBy[rayKey] = mapping.NodeLabel
		if _, conflict := workerGroup.Labels[rayKey]; conflict {
			return fmt.Errorf("worker group %s sets Ray label %q both in labels and in topology.labelMappings", groupName, rayKey)
		}
	}
	return nil
}

// BuildTopologyLabels maps node labels to Ray labels following the mappings. Every mapping must resolve to a
// non-empty allowlisted node label, otherwise the whole set fails
func BuildTopologyLabels(nodeLabels map[string]string, mappings []rayv1.TopologyLabelMapping, allowed sets.Set[string]) (map[string]string, error) {
	labels := make(map[string]string, len(mappings))
	var errList []string
	for _, mapping := range mappings {
		nodeLabelValue, ok := nodeLabels[mapping.NodeLabel]
		switch {
		case !allowed.Has(mapping.NodeLabel):
			errList = append(errList, fmt.Sprintf("node label %q is not in the operator's allowedNodeLabels", mapping.NodeLabel))
		case !ok:
			errList = append(errList, fmt.Sprintf("node lacks label %q", mapping.NodeLabel))
		case nodeLabelValue == "":
			errList = append(errList, fmt.Sprintf("node label %q is empty", mapping.NodeLabel))
		default:
			labels[TopologyRayLabelKey(mapping)] = nodeLabelValue
		}
	}
	if len(errList) > 0 {
		return nil, errors.New(strings.Join(errList, "; "))
	}
	return labels, nil
}
