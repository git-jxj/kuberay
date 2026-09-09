package v1

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

var rayClusterLog = logf.Log.WithName("raycluster-resource")

// SetupRayClusterWebhookWithManager registers the RayCluster webhook. allowedNodeLabels is the operator
// allowlist for topology.labelMappings
func SetupRayClusterWebhookWithManager(mgr ctrl.Manager, allowedNodeLabels []string) error {
	return ctrl.NewWebhookManagedBy(mgr, &rayv1.RayCluster{}).
		WithValidator(&RayClusterWebhook{AllowedNodeLabels: sets.New(allowedNodeLabels...)}).
		Complete()
}

type RayClusterWebhook struct {
	// AllowedNodeLabels is the operator allowlist for topology.labelMappings
	AllowedNodeLabels sets.Set[string]
}

//+kubebuilder:webhook:path=/validate-ray-io-v1-raycluster,mutating=false,failurePolicy=fail,sideEffects=None,groups=ray.io,resources=rayclusters,verbs=create;update,versions=v1,name=vraycluster.kb.io,admissionReviewVersions=v1

var _ admission.Validator[*rayv1.RayCluster] = &RayClusterWebhook{}

// ValidateCreate implements admission.Validator so a webhook will be registered for the type
func (w *RayClusterWebhook) ValidateCreate(_ context.Context, rayCluster *rayv1.RayCluster) (admission.Warnings, error) {
	rayClusterLog.Info("validate create", "name", rayCluster.Name)
	return nil, w.validateRayCluster(rayCluster)
}

// ValidateUpdate implements admission.Validator so a webhook will be registered for the type
func (w *RayClusterWebhook) ValidateUpdate(_ context.Context, _ *rayv1.RayCluster, rayCluster *rayv1.RayCluster) (admission.Warnings, error) {
	rayClusterLog.Info("validate update", "name", rayCluster.Name)
	return nil, w.validateRayCluster(rayCluster)
}

// ValidateDelete implements admission.Validator so a webhook will be registered for the type
func (w *RayClusterWebhook) ValidateDelete(_ context.Context, _ *rayv1.RayCluster) (admission.Warnings, error) {
	return nil, nil
}

func (w *RayClusterWebhook) validateRayCluster(rayCluster *rayv1.RayCluster) error {
	var allErrs field.ErrorList

	if err := utils.ValidateRayClusterMetadata(rayCluster.ObjectMeta); err != nil {
		allErrs = append(allErrs, field.Invalid(field.NewPath("metadata").Child("name"), rayCluster.Name, err.Error()))
	}

	if err := w.validateWorkerGroups(rayCluster); err != nil {
		allErrs = append(allErrs, err)
	}

	if err := w.validateTopology(rayCluster); err != nil {
		allErrs = append(allErrs, err)
	}

	if len(allErrs) == 0 {
		return nil
	}

	return apierrors.NewInvalid(
		schema.GroupKind{Group: "ray.io", Kind: "RayCluster"},
		rayCluster.Name, allErrs)
}

// validateTopology checks each worker group's topology, including the operator allowlist. The reconciler
// repeats the allowlist-independent rules
func (w *RayClusterWebhook) validateTopology(rayCluster *rayv1.RayCluster) *field.Error {
	for i := range rayCluster.Spec.WorkerGroupSpecs {
		group := &rayCluster.Spec.WorkerGroupSpecs[i]
		if group.Topology == nil {
			continue
		}
		path := field.NewPath("spec").Child("workerGroupSpecs").Index(i).Child("topology")
		if err := utils.ValidateWorkerGroupTopology(group, rayCluster.Spec.RayVersion); err != nil {
			return field.Invalid(path, *group.Topology, err.Error())
		}
		for j, mapping := range group.Topology.LabelMappings {
			if !w.AllowedNodeLabels.Has(mapping.NodeLabel) {
				return field.Forbidden(path.Child("labelMappings").Index(j).Child("nodeLabel"),
					fmt.Sprintf("node label %q is not in the operator's allowedNodeLabels", mapping.NodeLabel))
			}
		}
	}
	return nil
}

func (w *RayClusterWebhook) validateWorkerGroups(rayCluster *rayv1.RayCluster) *field.Error {
	workerGroupNames := make(map[string]bool)

	for i, workerGroup := range rayCluster.Spec.WorkerGroupSpecs {
		if _, ok := workerGroupNames[workerGroup.GroupName]; ok {
			return field.Invalid(field.NewPath("spec").Child("workerGroupSpecs").Index(i), workerGroup, "worker group names must be unique")
		}
		workerGroupNames[workerGroup.GroupName] = true
	}

	return nil
}
