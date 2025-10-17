/*
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cloudprovider

import (
	"context"
	stderr "errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/awslabs/operatorpkg/status"
	"github.com/oracle/oci-go-sdk/v65/core"
	"github.com/samber/lo"
	"github.com/zoom/karpenter-oci/pkg/apis/v1alpha1"
	cloudproviderevents "github.com/zoom/karpenter-oci/pkg/cloudprovider/events"
	"github.com/zoom/karpenter-oci/pkg/operator/options"
	"github.com/zoom/karpenter-oci/pkg/providers/imagefamily"
	"github.com/zoom/karpenter-oci/pkg/providers/instance"
	"github.com/zoom/karpenter-oci/pkg/providers/instancetype"
	"github.com/zoom/karpenter-oci/pkg/utils"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	coreapis "sigs.k8s.io/karpenter/pkg/apis"
	corev1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

var _ cloudprovider.CloudProvider = (*CloudProvider)(nil)

type CloudProvider struct {
	instanceTypeProvider *instancetype.Provider
	instanceProvider     *instance.Provider
	kubeClient           client.Client
	imageProvider        *imagefamily.Provider
	recorder             events.Recorder
}

func (c *CloudProvider) RepairPolicies() []cloudprovider.RepairPolicy {
	return []cloudprovider.RepairPolicy{
		// Supported Kubelet Node Conditions
		{
			ConditionType:      v1.NodeReady,
			ConditionStatus:    v1.ConditionFalse,
			TolerationDuration: 30 * time.Minute,
		},
		{
			ConditionType:      v1.NodeReady,
			ConditionStatus:    v1.ConditionUnknown,
			TolerationDuration: 30 * time.Minute,
		},
	}
}

func (c *CloudProvider) GetSupportedNodeClasses() []status.Object {
	return []status.Object{&v1alpha1.OciNodeClass{}}
}

func New(instanceTypeProvider *instancetype.Provider, instanceProvider *instance.Provider, recorder events.Recorder,
	kubeClient client.Client, imageProvider *imagefamily.Provider) *CloudProvider {
	return &CloudProvider{
		instanceTypeProvider: instanceTypeProvider,
		instanceProvider:     instanceProvider,
		kubeClient:           kubeClient,
		imageProvider:        imageProvider,
		recorder:             recorder,
	}
}

func (c *CloudProvider) Create(ctx context.Context, nodeClaim *corev1.NodeClaim) (*corev1.NodeClaim, error) {
	nodeClass, err := c.resolveNodeClassFromNodeClaim(ctx, nodeClaim)
	if err != nil {
		if errors.IsNotFound(err) {
			c.recorder.Publish(cloudproviderevents.NodeClaimFailedToResolveNodeClass(nodeClaim))
		}
		// We treat a failure to resolve the NodeClass as an ICE since this means there is no capacity possibilities for this NodeClaim
		return nil, cloudprovider.NewInsufficientCapacityError(fmt.Errorf("resolving node class, %w", err))
	}
	nodeClassReady := nodeClass.StatusConditions().Get(status.ConditionReady)
	if nodeClassReady.IsFalse() {
		return nil, cloudprovider.NewNodeClassNotReadyError(stderr.New(nodeClassReady.Message))
	}
	if nodeClassReady.IsUnknown() {
		return nil, fmt.Errorf("resolving NodeClass readiness, NodeClass is in Ready=Unknown, %s", nodeClassReady.Message)
	}
	instanceTypes, err := c.resolveInstanceTypes(ctx, nodeClaim, nodeClass)
	fmt.Println("instance types", len(instanceTypes))
	if err != nil {
		return nil, fmt.Errorf("resolving instance types, %w", err)
	}
	if len(instanceTypes) == 0 {
		return nil, cloudprovider.NewInsufficientCapacityError(fmt.Errorf("all requested instance types were unavailable during launch"))
	}
	newInstance, err := c.instanceProvider.Create(ctx, nodeClass, nodeClaim, instanceTypes)
	if err != nil {
		return nil, fmt.Errorf("creating instance, %w", err)
	}
	instanceType, _ := lo.Find(instanceTypes, func(i *cloudprovider.InstanceType) bool {
		fmt.Print(3839, i.Name, *newInstance.Shape)

		if strings.Contains(i.Name, "Flex") {

			re := regexp.MustCompile(`^(.*)-[0-9]+-[0-9]+$`)
			matches := re.FindStringSubmatch(i.Name)
			// fmt.Println(matches[1]) // Output: vm.standard.e4.flex
			shapeName := matches[1]
			return shapeName == *newInstance.Shape
		} else {
			return i.Name == *newInstance.Shape

		}

	})
	nc := c.instanceToNodeClaim(ctx, newInstance, instanceType)
	// fmt.Println("instanceToNodeClaim  ", nc)

	nc.Annotations = lo.Assign(nodeClass.Annotations, map[string]string{
		v1alpha1.AnnotationOciNodeClassHash:        nodeClass.Hash(),
		v1alpha1.AnnotationOciNodeClassHashVersion: v1alpha1.OciNodeClassHashVersion,
	})
	return nc, nil
}

func (c *CloudProvider) List(ctx context.Context) ([]*corev1.NodeClaim, error) {
	instances, err := c.instanceProvider.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing instances, %w", err)
	}
	var nodeClaims []*corev1.NodeClaim
	for _, instance := range instances {
		instanceType, err := c.resolveInstanceTypeFromInstance(ctx, &instance)
		if err != nil {
			return nil, fmt.Errorf("resolving instance type, %w", err)
		}
		nodeClaims = append(nodeClaims, c.instanceToNodeClaim(ctx, &instance, instanceType))
	}
	return nodeClaims, nil
}

func (c *CloudProvider) Get(ctx context.Context, providerID string) (*corev1.NodeClaim, error) {
	instance, err := c.instanceProvider.Get(ctx, providerID)
	if err != nil {
		return nil, fmt.Errorf("getting instance, %w", err)
	}
	instanceType, err := c.resolveInstanceTypeFromInstance(ctx, instance)
	if err != nil {
		return nil, fmt.Errorf("resolving instance type, %w", err)
	}
	return c.instanceToNodeClaim(ctx, instance, instanceType), nil
}

func (c *CloudProvider) LivenessProbe(req *http.Request) error {
	return nil
}

func (c *CloudProvider) GetInstanceTypes(ctx context.Context, nodePool *corev1.NodePool) ([]*cloudprovider.InstanceType, error) {
	if nodePool == nil {
		return c.instanceTypeProvider.List(ctx, &v1alpha1.OciNodeClass{})
	}
	nodeClass, err := c.resolveNodeClassFromNodePool(ctx, nodePool)
	if err != nil {
		if errors.IsNotFound(err) {
			c.recorder.Publish(cloudproviderevents.NodePoolFailedToResolveNodeClass(nodePool))
		}
		// We must return an error here in the event of the node class not being found. Otherwise users just get
		// no instance types and a failure to schedule with no indicator pointing to a bad configuration
		// as the cause.
		return nil, cloudprovider.NewInsufficientCapacityError(fmt.Errorf("resolving node class, %w", err))
	}
	instanceTypes, err := c.instanceTypeProvider.List(ctx, nodeClass)
	// for _, item := range instanceTypes {
	// fmt.Println(item.Name) // 报错：type *InstanceType has no field Name
	// if strings.HasSuffix(item.Name, "Flex") { // 仅当以 Flex 结尾时修改

	// cpuStr := item.Requirements.Get("karpenter.k8s.oracle/instance-cpu").Values()[0]
	// memStr := item.Requirements.Get("karpenter.k8s.oracle/instance-memory").Values()[0]

	// item.Name = fmt.Sprintf("%s-%s-%s", item.Name, cpuStr, memStr)
	// fmt.Println(item.Name) // 报错：type *InstanceType has no field Name

	// }
	// }

	if err != nil {
		return nil, err
	}
	if len(instanceTypes) == 0 {
		return nil, cloudprovider.NewInsufficientCapacityError(fmt.Errorf("all requested instance types were unavailable during launch"))
	}
	return instanceTypes, nil
}

func (c *CloudProvider) Delete(ctx context.Context, nodeClaim *corev1.NodeClaim) error {
	ctx = log.IntoContext(ctx, log.FromContext(ctx).WithValues("id", nodeClaim.Status.ProviderID))
	return c.instanceProvider.Delete(ctx, nodeClaim.Status.ProviderID)
}

func (c *CloudProvider) AlignNodeFlexShapeLabel(ctx context.Context) {

	fmt.Print("12931231023123123")

	var nodeList v1.NodeList
	if err := c.kubeClient.List(ctx, &nodeList); err != nil {
		fmt.Print(err)
	}

	// 过滤出 karpenter.sh/initialized=true 的节点
	var targetNodes []string
	for _, node := range nodeList.Items {
		if node.Labels["karpenter.sh/initialized"] == "true" {
			targetNodes = append(targetNodes, node.Name)
		}
	}

	if len(targetNodes) == 0 {
		fmt.Println("No nodes with karpenter.sh/initialized=true found")
		return
	}

	// 更新节点的 node.kubernetes.io/instance-type label
	for _, nodeName := range targetNodes {
		var node v1.Node
		if err := c.kubeClient.Get(ctx, types.NamespacedName{Name: nodeName}, &node); err != nil {
			fmt.Printf("Error getting node %s: %v", nodeName, err)
			continue
		}

		instanceType := node.Labels["node.kubernetes.io/instance-type"]
		instanceCPU := node.Labels["karpenter.k8s.oracle/instance-cpu"]
		instanceMemory := node.Labels["karpenter.k8s.oracle/instance-memory"]

		if strings.Contains(instanceType, "Flex") {
			newInstanceType := fmt.Sprintf("%s-%s-%s", instanceType, instanceCPU, instanceMemory)
			node.Labels["node.kubernetes.io/instance-type"] = newInstanceType

			if err := c.kubeClient.Patch(ctx, &node, client.Merge); err != nil {
				fmt.Printf("Error updating node %s: %v", nodeName, err)
			} else {
				fmt.Printf("Updated node %s with new instance type: %s", nodeName, newInstanceType)
			}
		} else {
			fmt.Printf("Node %s instance type %s does not contain 'Flex', skipping update", nodeName, instanceType)
		}

	}
	for _, nodeName := range targetNodes {
		var node v1.Node
		if err := c.kubeClient.Get(ctx, types.NamespacedName{Name: nodeName}, &node); err != nil {
			fmt.Printf("Error getting node %s: %v", nodeName, err)
			continue
		}

		instanceType := node.Labels["node.kubernetes.io/instance-type"]

		if strings.Contains(instanceType, "Flex") {
			fmt.Printf("Updated node22 %s with new instance type: %s", nodeName, instanceType)

		}

	}

}
func (c *CloudProvider) IsDrifted(ctx context.Context, nodeClaim *corev1.NodeClaim) (cloudprovider.DriftReason, error) {
	c.AlignNodeFlexShapeLabel(ctx)
	// get node pool using pool name parsed from node claim label
	nodePoolName, ok := nodeClaim.Labels[corev1.NodePoolLabelKey]
	if !ok {
		return "", nil
	}
	nodePool := &corev1.NodePool{}
	if err := c.kubeClient.Get(ctx, types.NamespacedName{Name: nodePoolName}, nodePool); err != nil {
		return "", client.IgnoreNotFound(err)
	}
	if nodePool.Spec.Template.Spec.NodeClassRef == nil {
		return "", nil
	}
	// get node class related to node pool
	nodeClass, err := c.resolveNodeClassFromNodePool(ctx, nodePool)
	if err != nil {
		if errors.IsNotFound(err) {
			c.recorder.Publish(cloudproviderevents.NodePoolFailedToResolveNodeClass(nodePool))
		}
		return "", client.IgnoreNotFound(fmt.Errorf("resolving node class, %w", err))
	}

	// check drift reason
	driftReason, err := c.isNodeClassDrifted(ctx, nodeClaim, nodePool, nodeClass)
	if err != nil {
		return "", err
	}
	return driftReason, nil
}

func (c *CloudProvider) Name() string {
	return "oracle"
}

func (c *CloudProvider) resolveNodeClassFromNodeClaim(ctx context.Context, nodeClaim *corev1.NodeClaim) (*v1alpha1.OciNodeClass, error) {
	nodeClass := &v1alpha1.OciNodeClass{}
	if err := c.kubeClient.Get(ctx, types.NamespacedName{Name: nodeClaim.Spec.NodeClassRef.Name}, nodeClass); err != nil {
		return nil, err
	}
	// For the purposes of NodeClass CloudProvider resolution, we treat deleting NodeClasses as NotFound
	if !nodeClass.DeletionTimestamp.IsZero() {
		// For the purposes of NodeClass CloudProvider resolution, we treat deleting NodeClasses as NotFound,
		// but we return a different error message to be clearer to users
		return nil, newTerminatingNodeClassError(nodeClass.Name)
	}
	return nodeClass, nil
}

func (c *CloudProvider) resolveNodeClassFromNodePool(ctx context.Context, nodePool *corev1.NodePool) (*v1alpha1.OciNodeClass, error) {
	nodeClass := &v1alpha1.OciNodeClass{}
	if err := c.kubeClient.Get(ctx, types.NamespacedName{Name: nodePool.Spec.Template.Spec.NodeClassRef.Name}, nodeClass); err != nil {
		return nil, err
	}
	if !nodeClass.DeletionTimestamp.IsZero() {
		// For the purposes of NodeClass CloudProvider resolution, we treat deleting NodeClasses as NotFound,
		// but we return a different error message to be clearer to users
		return nil, newTerminatingNodeClassError(nodeClass.Name)
	}
	return nodeClass, nil
}

func (c *CloudProvider) resolveInstanceTypes(ctx context.Context, nodeClaim *corev1.NodeClaim, nodeClass *v1alpha1.OciNodeClass) ([]*cloudprovider.InstanceType, error) {
	instanceTypes, err := c.instanceTypeProvider.List(ctx, nodeClass)
	if err != nil {
		return nil, fmt.Errorf("getting instance types, %w", err)
	}
	reqs := scheduling.NewNodeSelectorRequirementsWithMinValues(nodeClaim.Spec.Requirements...)
	return lo.Filter(instanceTypes, func(i *cloudprovider.InstanceType, _ int) bool {
		// fmt.Println(12323, reqs.Compatible(i.Requirements, scheduling.AllowUndefinedWellKnownLabels) == nil)
		// fmt.Println("filts", resources.Fits(nodeClaim.Spec.Resources.Requests, i.Allocatable()))
		return reqs.Compatible(i.Requirements, scheduling.AllowUndefinedWellKnownLabels) == nil &&
			len(i.Offerings.Compatible(reqs).Available()) > 0 &&
			resources.Fits(nodeClaim.Spec.Resources.Requests, i.Allocatable())
	}), nil
}

func (c *CloudProvider) resolveInstanceTypeFromInstance(ctx context.Context, instance *core.Instance) (*cloudprovider.InstanceType, error) {
	nodePool, err := c.resolveNodePoolFromInstance(ctx, instance)
	if err != nil {
		// If we can't resolve the NodePool, we fall back to not getting instance type info
		return nil, client.IgnoreNotFound(fmt.Errorf("resolving nodepool, %w", err))
	}
	instanceTypes, err := c.GetInstanceTypes(ctx, nodePool)
	if err != nil {
		// If we can't resolve the NodePool, we fall back to not getting instance type info
		return nil, client.IgnoreNotFound(fmt.Errorf("resolving nodeclass, %w", err))
	}
	instanceType, _ := lo.Find(instanceTypes, func(i *cloudprovider.InstanceType) bool {
		if strings.Contains(*instance.Shape, "Flex") {

			re := regexp.MustCompile(`^(.*)-[0-9]+-[0-9]+$`)
			matches := re.FindStringSubmatch(i.Name)
			// fmt.Println(matches[1]) // Output: vm.standard.e4.flex
			shapeName := matches[1]
			return shapeName == *instance.Shape
		} else {
			return i.Name == *instance.Shape

		}

	})
	return instanceType, nil
}

func (c *CloudProvider) resolveNodePoolFromInstance(ctx context.Context, instance *core.Instance) (*corev1.NodePool, error) {
	if nodePoolName, ok := instance.DefinedTags[options.FromContext(ctx).TagNamespace][utils.SafeTagKey(corev1.NodePoolLabelKey)]; ok {
		nodePool := &corev1.NodePool{}
		if err := c.kubeClient.Get(ctx, types.NamespacedName{Name: nodePoolName.(string)}, nodePool); err != nil {
			return nil, err
		}
		return nodePool, nil
	}
	return nil, errors.NewNotFound(schema.GroupResource{Group: coreapis.Group, Resource: "nodepools"}, "")
}

func (c *CloudProvider) instanceToNodeClaim(ctx context.Context, i *core.Instance, instanceType *cloudprovider.InstanceType) *corev1.NodeClaim {
	nodeClaim := &corev1.NodeClaim{}
	labels := map[string]string{}
	annotations := map[string]string{}

	if instanceType != nil {
		for key, req := range instanceType.Requirements {
			if req.Len() == 1 {
				labels[key] = req.Values()[0]
			}
		}
		resourceFilter := func(n v1.ResourceName, v resource.Quantity) bool {
			return !resources.IsZero(v)
		}
		nodeClaim.Status.Capacity = utils.FilterMap(instanceType.Capacity, resourceFilter)
		nodeClaim.Status.Allocatable = utils.FilterMap(instanceType.Allocatable(), resourceFilter)
	}
	// remove the prefix in AvailabilityDomain
	splitAd := strings.Split(lo.FromPtr(i.AvailabilityDomain), ":")
	if len(splitAd) == 2 {
		labels[v1.LabelTopologyZone] = splitAd[1]
	} else {
		labels[v1.LabelTopologyZone] = splitAd[0]
	}
	// can't use this deprecated label, conflict with zone, refer: NewRequirementWithFlexibility
	// labels[v1.LabelFailureDomainBetaZone] = *i.FaultDomain

	labels[corev1.CapacityTypeLabelKey] = corev1.CapacityTypeOnDemand
	if i.PreemptibleInstanceConfig != nil {
		labels[corev1.CapacityTypeLabelKey] = v1alpha1.CapacityTypePreemptible
	}
	if v, ok := i.DefinedTags[options.FromContext(ctx).TagNamespace][utils.SafeTagKey(corev1.NodePoolLabelKey)]; ok {
		labels[corev1.NodePoolLabelKey] = v.(string)
	}
	if v, ok := i.DefinedTags[options.FromContext(ctx).TagNamespace][utils.SafeTagKey(v1alpha1.ManagedByAnnotationKey)]; ok {
		annotations[v1alpha1.ManagedByAnnotationKey] = v.(string)
	}
	// Set the deletionTimestamp to be the current time if the instance is currently terminating
	if i.LifecycleState == core.InstanceLifecycleStateTerminating || i.LifecycleState == core.InstanceLifecycleStateTerminated {
		nodeClaim.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	}
	nodeClaim.Labels = labels
	nodeClaim.Annotations = annotations
	nodeClaim.CreationTimestamp = metav1.Time{Time: i.TimeCreated.Time}
	nodeClaim.Status.ProviderID = *i.Id
	nodeClaim.Status.ImageID = *i.SourceDetails.(core.InstanceSourceViaImageDetails).ImageId
	return nodeClaim
}

// newTerminatingNodeClassError returns a NotFound error for handling by
func newTerminatingNodeClassError(name string) *errors.StatusError {
	qualifiedResource := schema.GroupResource{Group: v1alpha1.Group, Resource: "ocinodeclasses"}
	err := errors.NewNotFound(qualifiedResource, name)
	err.ErrStatus.Message = fmt.Sprintf("%s %q is terminating, treating as not found", qualifiedResource.String(), name)
	return err
}
