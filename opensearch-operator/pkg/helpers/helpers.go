package helpers

import (
	"context"
	"errors"
	"fmt"
	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/types"
	"opensearch.opster.io/pkg/metrics"
	"reflect"
	"sort"
	"time"

	"github.com/hashicorp/go-version"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	opsterv1 "opensearch.opster.io/api/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	stsUpdateWaitTime = 30
	updateStepTime    = 3
)

func ContainsString(slice []string, s string) bool {
	for _, item := range slice {
		if item == s {
			return true
		}
	}
	return false

}

func GetField(v *appsv1.StatefulSetSpec, field string) interface{} {

	r := reflect.ValueOf(v)
	f := reflect.Indirect(r).FieldByName(field).Interface()
	return f
}

func RemoveIt(ss opsterv1.ComponentStatus, ssSlice []opsterv1.ComponentStatus) []opsterv1.ComponentStatus {
	for idx, v := range ssSlice {
		if v == ss {
			return append(ssSlice[0:idx], ssSlice[idx+1:]...)
		}
	}
	return ssSlice
}
func Replace(remove opsterv1.ComponentStatus, add opsterv1.ComponentStatus, ssSlice []opsterv1.ComponentStatus) []opsterv1.ComponentStatus {
	removedSlice := RemoveIt(remove, ssSlice)
	fullSliced := append(removedSlice, add)
	return fullSliced
}

func FindFirstPartial(
	arr []opsterv1.ComponentStatus,
	item opsterv1.ComponentStatus,
	predicator func(opsterv1.ComponentStatus, opsterv1.ComponentStatus) (opsterv1.ComponentStatus, bool),
) (opsterv1.ComponentStatus, bool) {
	for i := 0; i < len(arr); i++ {
		itemInArr, found := predicator(arr[i], item)
		if found {
			return itemInArr, found
		}
	}
	return item, false
}

func FindByPath(obj interface{}, keys []string) (interface{}, bool) {
	mobj, ok := obj.(map[string]interface{})
	if !ok {
		return nil, false
	}
	for i := 0; i < len(keys)-1; i++ {
		if currentVal, found := mobj[keys[i]]; found {
			subPath, ok := currentVal.(map[string]interface{})
			if !ok {
				return nil, false
			}
			mobj = subPath
		}
	}
	val, ok := mobj[keys[len(keys)-1]]
	return val, ok
}

func UsernameAndPassword(ctx context.Context, k8sClient client.Client, cr *opsterv1.OpenSearchCluster) (string, string, error) {
	if cr.Spec.Security != nil && cr.Spec.Security.Config != nil && cr.Spec.Security.Config.AdminCredentialsSecret.Name != "" {
		// Read credentials from secret
		credentialsSecret := corev1.Secret{}
		if err := k8sClient.Get(ctx, client.ObjectKey{Name: cr.Spec.Security.Config.AdminCredentialsSecret.Name, Namespace: cr.Namespace}, &credentialsSecret); err != nil {
			return "", "", err
		}
		username, usernameExists := credentialsSecret.Data["username"]
		password, passwordExists := credentialsSecret.Data["password"]
		if !usernameExists || !passwordExists {
			return "", "", errors.New("username or password field missing")
		}
		return string(username), string(password), nil
	} else {
		// Use default demo credentials
		return "admin", "admin", nil
	}
}

func GetByDescriptionAndGroup(left opsterv1.ComponentStatus, right opsterv1.ComponentStatus) (opsterv1.ComponentStatus, bool) {
	if left.Description == right.Description && left.Component == right.Component {
		return left, true
	}
	return right, false
}

func MergeConfigs(left map[string]string, right map[string]string) map[string]string {
	if left == nil {
		return right
	}
	for k, v := range right {
		left[k] = v
	}
	return left
}

// Return the keys of the input map in sorted order
// Can be used if you want to iterate over a map but have a stable order
func SortedKeys(input map[string]string) []string {
	keys := make([]string, 0, len(input))
	for key := range input {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func ResolveClusterManagerRole(ver string) string {
	masterRole := "master"
	osVer, err := version.NewVersion(ver)

	clusterManagerVer, _ := version.NewVersion("2.0.0")
	if err == nil && osVer.GreaterThanOrEqual(clusterManagerVer) {
		masterRole = "cluster_manager"
	}
	return masterRole
}

// Map any cluster roles that have changed between major OpenSearch versions
func MapClusterRole(role string, ver string) string {
	osVer, err := version.NewVersion(ver)
	if err != nil {
		return role
	}
	clusterManagerVer, _ := version.NewVersion("2.0.0")
	is2XVersion := osVer.GreaterThanOrEqual(clusterManagerVer)
	if role == "master" && is2XVersion {
		return "cluster_manager"
	} else if role == "cluster_manager" && !is2XVersion {
		return "master"
	} else {
		return role
	}
}

func MapClusterRoles(roles []string, version string) []string {
	mapped_roles := []string{}
	for _, role := range roles {
		mapped_roles = append(mapped_roles, MapClusterRole(role, version))
	}
	return mapped_roles
}

// Get leftSlice strings not in rightSlice
func DiffSlice(leftSlice, rightSlice []string) []string {
	//diff := []string{}
	var diff []string

	for _, leftSliceString := range leftSlice {
		if !ContainsString(rightSlice, leftSliceString) {
			diff = append(diff, leftSliceString)
		}
	}
	return diff
}

// Count the number of PVCs created for the given NodePool
func CountPVCsForNodePool(ctx context.Context, k8sClient client.Client, cr *opsterv1.OpenSearchCluster, nodePool *opsterv1.NodePool) (int, error) {
	clusterReq, err := labels.NewRequirement(ClusterLabel, selection.Equals, []string{cr.ObjectMeta.Name})
	if err != nil {
		return 0, err
	}
	componentReq, err := labels.NewRequirement(NodePoolLabel, selection.Equals, []string{nodePool.Component})
	if err != nil {
		return 0, err
	}
	selector := labels.NewSelector()
	selector = selector.Add(*clusterReq, *componentReq)
	list := corev1.PersistentVolumeClaimList{}
	if err := k8sClient.List(ctx, &list, &client.ListOptions{LabelSelector: selector}); err != nil {
		return 0, err
	}
	return len(list.Items), nil
}

// Delete a STS with cascade=orphan and wait until it is actually deleted from the kubernetes API
func WaitForSTSDelete(ctx context.Context, k8sClient client.Client, obj *appsv1.StatefulSet) error {
	opts := client.DeleteOptions{}
	client.PropagationPolicy(metav1.DeletePropagationOrphan).ApplyToDelete(&opts)
	if err := k8sClient.Delete(ctx, obj, &opts); err != nil {
		return err
	}
	for i := 1; i <= stsUpdateWaitTime/updateStepTime; i++ {
		existing := appsv1.StatefulSet{}
		err := k8sClient.Get(ctx, client.ObjectKeyFromObject(obj), &existing)
		if err != nil {
			return nil
		}
		time.Sleep(time.Second * updateStepTime)
	}
	return fmt.Errorf("failed to delete STS")
}

// Wait for max 30s until a STS has at least the given number of replicas
func WaitForSTSReplicas(ctx context.Context, k8sClient client.Client, obj *appsv1.StatefulSet, replicas int32) error {
	for i := 1; i <= stsUpdateWaitTime/updateStepTime; i++ {
		existing := appsv1.StatefulSet{}
		err := k8sClient.Get(ctx, client.ObjectKeyFromObject(obj), &existing)
		if err == nil {
			if existing.Status.Replicas >= replicas {
				return nil
			}
		}
		time.Sleep(time.Second * updateStepTime)
	}
	return fmt.Errorf("failed to wait for replicas")
}

// Wait for max 30s until a STS has a normal status (CurrentRevision != "")
func WaitForSTSStatus(ctx context.Context, k8sClient client.Client, obj *appsv1.StatefulSet) (*appsv1.StatefulSet, error) {
	for i := 1; i <= stsUpdateWaitTime/updateStepTime; i++ {
		existing := appsv1.StatefulSet{}
		err := k8sClient.Get(ctx, client.ObjectKeyFromObject(obj), &existing)
		if err == nil {
			if existing.Status.CurrentRevision != "" {
				return &existing, nil
			}
		}
		time.Sleep(time.Second * updateStepTime)
	}
	return nil, fmt.Errorf("failed to wait for STS")
}

// GetSTSForNodePool returns the corresponding sts for a given nodePool and cluster name
func GetSTSForNodePool(ctx context.Context, k8sClient client.Client, nodePool opsterv1.NodePool, clusterName, clusterNamespace string) (*appsv1.StatefulSet, error) {
	sts := &appsv1.StatefulSet{}
	stsName := clusterName + "-" + nodePool.Component

	err := k8sClient.Get(ctx, types.NamespacedName{Name: stsName, Namespace: clusterNamespace}, sts)

	return sts, err
}

// DeleteSTSForNodePool deletes the sts for the corresponding nodePool
func DeleteSTSForNodePool(ctx context.Context, k8sClient client.Client, nodePool opsterv1.NodePool, clusterName, clusterNamespace string) error {

	sts, err := GetSTSForNodePool(ctx, k8sClient, nodePool, clusterName, clusterNamespace)
	if err != nil {
		return err
	}

	opts := client.DeleteOptions{}
	// Add this so pods of the sts are deleted as well, otherwise they would remain as orphaned pods
	client.PropagationPolicy(metav1.DeletePropagationForeground).ApplyToDelete(&opts)

	err = k8sClient.Delete(ctx, sts, &opts)

	return err
}

// DeleteSecurityUpdateJob deletes the securityconfig update job
func DeleteSecurityUpdateJob(ctx context.Context, k8sClient client.Client, clusterName, clusterNamespace string) error {
	jobName := clusterName + "-securityconfig-update"
	job := batchv1.Job{}
	err := k8sClient.Get(ctx, client.ObjectKey{Name: jobName, Namespace: clusterNamespace}, &job)

	if err != nil {
		return err
	}

	opts := client.DeleteOptions{}
	// Add this so pods of the job are deleted as well, otherwise they would remain as orphaned pods
	client.PropagationPolicy(metav1.DeletePropagationForeground).ApplyToDelete(&opts)
	err = k8sClient.Delete(ctx, &job, &opts)

	return err
}

func HasDataRole(nodePool *opsterv1.NodePool) bool {
	return ContainsString(nodePool.Roles, "data")
}

func HasManagerRole(nodePool *opsterv1.NodePool) bool {
	return ContainsString(nodePool.Roles, "master") || ContainsString(nodePool.Roles, "cluster_manager")
}

func RemoveDuplicateStrings(strSlice []string) []string {
	allKeys := make(map[string]bool)
	list := []string{}
	for _, item := range strSlice {
		if _, value := allKeys[item]; !value {
			allKeys[item] = true
			list = append(list, item)
		}
	}
	return list
}

// Compares whether v1 is LessThan v2
func CompareVersions(v1 string, v2 string) bool {
	ver1, err := version.NewVersion(v1)
	ver2, _ := version.NewVersion(v2)
	return err == nil && ver1.LessThan(ver2)
}

func GetAutoscalingPolicy(ctx context.Context, k8sClient client.Client, nodePool *opsterv1.NodePool, instance *opsterv1.OpenSearchCluster) (*opsterv1.Autoscaler, error) {
	var policy string
	//if a cluster level policy is defined, default to that over nodepool
	if instance.Spec.General.AutoScaler.ClusterAutoScalePolicy != "" {
		policy = instance.Spec.General.AutoScaler.ClusterAutoScalePolicy
		//if a nodePool level policy is defined
	} else if nodePool.AutoScalePolicy != "" {
		policy = nodePool.AutoScalePolicy
		//if no policy is defined, continue running and warn the user
	} else {
		return nil, nil
	}
	autoscaler := &opsterv1.Autoscaler{}
	err := k8sClient.Get(ctx, types.NamespacedName{
		Name:      policy,
		Namespace: instance.Namespace,
	}, autoscaler)
	if err != nil {
		return nil, err
	}
	return autoscaler, nil
}

func EvalScalingTime(component string, instance *opsterv1.OpenSearchCluster) (bool, error) {
	autoscaleStatus := instance.Status.Scaler
	//check if scaleTimeout is missing, default to 10m if it is.
	var scaleTimeout string
	if instance.Spec.General.AutoScaler.ScaleTimeout == "" {
		scaleTimeout = "10m"
	} else {
		scaleTimeout = instance.Spec.General.AutoScaler.ScaleTimeout
	}
	//get duration; if empty invalid format string defaults to 0s
	duration, err := time.ParseDuration(scaleTimeout)
	if err != nil {
		return false, fmt.Errorf("Unable to parse scaleTimeout: %v", err)
	}
	//if the cluster has existed longer than the scaleInterval; UTC used to match OS cluster creation timestamp
	if time.Now().UTC().After(instance.CreationTimestamp.Add(duration)) {
		//if the key exists in the map
		if existingLastScaleTime, ok := autoscaleStatus[component]; ok {
			targetTime := existingLastScaleTime.LastScaleTime.Add(duration)
			//if now is after the lastScaleTime + duration
			if time.Now().UTC().After(targetTime) {
				return true, nil
			}
		} else {
			//scaling allowed
			return true, nil
		}
	}
	//scaling not allowed
	return false, nil
}

func EvalScalingRules(queryEvaluator metrics.ScalingQueryEvaluator, nodePool *opsterv1.NodePool, autoscalerPolicy *opsterv1.Autoscaler, instance *opsterv1.OpenSearchCluster) (int32, error) {
	scaleCount := int32(0)
	for r, rule := range autoscalerPolicy.Spec.Rules {
		//if the rule and nodetype do not match, break to the next ruleEval
		if !ContainsString(nodePool.Roles, rule.NodeRole) {
			break
		}
		//if the rule is disabled go on to the next rule eval
		if !rule.Behavior.Enable {
			continue
		}
		//if the ruleSet ever evaluates to false the scaling decision will not occur
		ruleEval := false
		//iterate through items for the relevant nodeRole type
		for _, item := range rule.Items {
			query, err := buildPrometheusQuery(item, instance.Name, nodePool.Component)
			if err != nil {
				return 0, err
			}

			ruleEval, err = queryEvaluator.Eval(context.Background(), instance.Spec.General.AutoScaler.PrometheusEndpoint, query)
			if err != nil {
				return 0, err
			}
			if !ruleEval {
				break
			}
		}
		//if any ruleset evals to true, we are going to scale;
		if ruleEval {
			//if max replicas are set for both scalingActions then we know there is a problem with the config
			if rule.Behavior.ScaleUp.MaxReplicas != 0 && rule.Behavior.ScaleDown.MaxReplicas != 0 {
				return 0, fmt.Errorf("Both scaleUp and scaleDown logic enabled for rule[%v] in %v autoscaler policy. ", r, autoscalerPolicy.Name)
			}
			//if the current replicas is less than the defined MaxReplicas, scale up
			if instance.Status.Scaler[nodePool.Component].Replicas < rule.Behavior.ScaleUp.MaxReplicas {
				scaleCount++
			}
			//if the current replicas is greater than the defined nodePool replicas, scale down
			if instance.Status.Scaler[nodePool.Component].Replicas > nodePool.Replicas && rule.Behavior.ScaleDown.MaxReplicas != 0 {
				scaleCount--
			}
		}
	}
	return scaleCount, nil
}

func buildPrometheusQuery(item opsterv1.Item, instanceName string, nodeComponent string) (string, error) {
	query := item.Metric
	nodeMatcher := "node=~\"" + instanceName + "-" + nodeComponent + "-[0-9]+$\""
	//build nodeMatcher string
	if item.QueryOptions.LabelMatchers != nil {
		for i, labelMatcher := range item.QueryOptions.LabelMatchers {
			if i == 0 {
				query = query + "{"
			}
			query = query + labelMatcher
		}
		query = query + "," + nodeMatcher + "}"
	} else {
		query = query + "{" + nodeMatcher + "}"
	}
	//add time interval if exists; a function wrapper must exist
	if &item.QueryOptions.Interval != nil && &item.QueryOptions.Function != nil {
		query = query + "[" + item.QueryOptions.Interval + "]"
	} else {
		return "", fmt.Errorf("A function wrapper is required when using intervals for Prometheus query. ")
	}
	//add func wrapper if exists
	if &item.QueryOptions.Function != nil {
		query = item.QueryOptions.Function + "(" + query + ")"
	}
	if item.QueryOptions.AggregateEvaluation {
		query = "avg(" + query + ")"
	}
	//do boolean threshold comparison
	query = query + " " + item.Operator + "bool " + item.Threshold
	return query, nil
}
