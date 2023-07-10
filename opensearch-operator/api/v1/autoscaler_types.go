/*
Copyright 2021.

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

package v1

import (
	"context"
	"fmt"
	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"time"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// AutoscalerSpec defines the desired state of Autoscaler
type AutoscalerSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	Rules []Rule `json:"rules,omitempty"`
}

// Rule defines the contents of an autoscaler rule
type Rule struct {
	NodeRole string `json:"nodeRole"`
	Behavior Scale  `json:"behavior"`
	Items    []Item `json:"items"`
}

// Scale defines the behaviors of the scaling rule
type Scale struct {
	Enable    bool      `json:"enable"`
	ScaleUp   ScaleConf `json:"scaleUp,omitempty"`
	ScaleDown ScaleConf `json:"scaleDown,omitempty"`
}

// ScaleConf defines configuration rules for scaling as needed
type ScaleConf struct {
	MaxReplicas int32 `json:"maxReplicas,omitempty"`
}

// Item defines the contents of an autoscaler rule item
type Item struct {
	Metric       string       `json:"metric"`
	Operator     string       `json:"operator"`
	Threshold    string       `json:"threshold"`
	QueryOptions QueryOptions `json:"queryOptions,omitempty"`
}

// QueryOptions defines the components to build a prometheus query for a scaling item
type QueryOptions struct {
	LabelMatchers       []string `json:"labelMatchers,omitempty"`
	Function            string   `json:"function,omitempty"`
	Interval            string   `json:"interval,omitempty"`
	AggregateEvaluation bool     `json:"aggregateEvaluation,omitempty"`
}

// AutoscalerStatus defines the observed state of Autoscaler
type AutoscalerStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file
	Component   string `json:"component,omitempty"`
	Status      string `json:"status,omitempty"`
	Description string `json:"description,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:resource:shortName=autoscaler
//+kubebuilder:subresource:status

// Autoscaler is the Schema for the autoscalers API
type Autoscaler struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AutoscalerSpec   `json:"spec,omitempty"`
	Status AutoscalerStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// AutoscalerList contains a list of Autoscaler
type AutoscalerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Autoscaler `json:"items"`
}

type AutoscalerController interface {
	Eval(ctx context.Context, prometheusUrl string, query string) (bool, error)
}

func (a *Autoscaler) BuildQuery(ctx context.Context, item Item, instance *OpenSearchCluster, nodePool *NodePool) (string, error) {
	query := item.Metric
	nodeMatcher := "node=~\"" + instance.Name + "-" + nodePool.Component + "-[0-9]+$\""
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

func (a *Autoscaler) Eval(ctx context.Context, prometheusUrl string, query string) (bool, error) {
	apiClient, err := NewPrometheusClient(prometheusUrl)
	if err != nil {
		return false, fmt.Errorf("unable to create Prometheus client: %v", err)
	}

	result, warnings, err := apiClient.Query(context.Background(), query, time.Now())
	if err != nil { //if the query fails we will not make a scaling decision
		return false, fmt.Errorf("Prometheus query [ %q ] failed with error: %v ", query, err)
	}
	if len(warnings) > 0 { //if there are warnings we will not make a scaling decision
		return false, fmt.Errorf("warnings received: %v", err)
	}

	// Check the result type and iterate over the data
	if result.Type() != model.ValVector {
		return false, fmt.Errorf("prometheus result type not a Vector: %v", err)
	} else {
		//if all values a true set ruleEval, else break out of the itemloop and check the next rule
		for _, vector := range result.(model.Vector) {
			if vector.Value == 1 {
				return true, nil
			} else {
				return false, nil
			}
		}
	}

	return false, nil
}

func NewPrometheusClient(prometheusEndpoint string) (v1.API, error) {
	client, err := api.NewClient(api.Config{
		Address: prometheusEndpoint,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create client")
	}

	v1api := v1.NewAPI(client)
	return v1api, nil
}

func init() {
	SchemeBuilder.Register(&Autoscaler{}, &AutoscalerList{})
}
