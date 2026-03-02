// Copyright Amazon.com Inc. or its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"). You may
// not use this file except in compliance with the License. A copy of the
// License is located at
//
//     http://aws.amazon.com/apache2.0/
//
// or in the "license" file accompanying this file. This file is distributed
// on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
// express or implied. See the License for the specific language governing
// permissions and limitations under the License.

package application

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"

	ackrtlog "github.com/aws-controllers-k8s/runtime/pkg/runtime/log"
	acktypes "github.com/aws-controllers-k8s/runtime/pkg/types"

	"github.com/aws-controllers-k8s/emrserverless-controller/pkg/tags"
)

// resourceTagManager holds methods for working with AWS resource tags
type resourceTagManager struct {
	// tagManager provides methods for working with AWS resource tags
	tagManager *tags.TagManager
	// logConstructor contains a method that can produce a logger for a
	// resource manager from a supplied context.
	logConstructor func(context.Context) acktypes.Logger
}

// newResourceTagManager returns a new resourceTagManager struct
func newResourceTagManager(
	cfg aws.Config,
	logConstructor func(context.Context) acktypes.Logger,
) *resourceTagManager {
	return &resourceTagManager{
		tagManager:     tags.NewTagManager(cfg, logConstructor),
		logConstructor: logConstructor,
	}
}

// getTags returns the tags for a given resource ARN
func (rtm *resourceTagManager) getTags(
	ctx context.Context,
	resourceARN string,
) map[string]string {
	tags, err := rtm.tagManager.GetTags(ctx, resourceARN)
	if err != nil {
		rtm.logConstructor(ctx).Debug("error getting tags for resource", "error", err)
		return nil
	}
	return tags
}

// syncTags synchronizes tags between the supplied desired and latest resources
func (rtm *resourceTagManager) syncTags(
	ctx context.Context,
	latest *resource,
	desired *resource,
) error {
	if latest.ko.Status.ACKResourceMetadata == nil || latest.ko.Status.ACKResourceMetadata.ARN == nil {
		return nil
	}
	resourceARN := string(*latest.ko.Status.ACKResourceMetadata.ARN)

	latestTags := make(map[string]string)
	if latest.ko.Spec.Tags != nil {
		for k, v := range latest.ko.Spec.Tags {
			if v != nil {
				latestTags[k] = *v
			}
		}
	}

	desiredTags := make(map[string]string)
	if desired.ko.Spec.Tags != nil {
		for k, v := range desired.ko.Spec.Tags {
			if v != nil {
				desiredTags[k] = *v
			}
		}
	}

	_, err := rtm.tagManager.SyncTags(ctx, resourceARN, desiredTags, latestTags)
	if err != nil {
		return err
	}

	// Update the latest resource's tags to match the desired tags
	latest.ko.Spec.Tags = desired.ko.Spec.Tags

	return nil
}

// getTags returns the tags for a given resource ARN
// This is a wrapper method that delegates to resourceTagManager
func (rm *resourceManager) getTags(
	ctx context.Context,
	resourceARN string,
) map[string]*string {
	logConstructor := func(ctx context.Context) acktypes.Logger {
		return ackrtlog.FromContext(ctx)
	}
	rtm := newResourceTagManager(rm.clientcfg, logConstructor)
	sdkTags := rtm.getTags(ctx, resourceARN)

	if sdkTags == nil {
		return nil
	}

	// Convert SDK tags (map[string]string) to CRD tags (map[string]*string)
	tags := make(map[string]*string)
	for k, v := range sdkTags {
		val := v
		tags[k] = &val
	}
	return tags
}

// syncTags synchronizes tags between the supplied desired and latest resources
// This is a wrapper method that delegates to resourceTagManager
func (rm *resourceManager) syncTags(
	ctx context.Context,
	latest *resource,
	desired *resource,
) error {
	logConstructor := func(ctx context.Context) acktypes.Logger {
		return ackrtlog.FromContext(ctx)
	}
	rtm := newResourceTagManager(rm.clientcfg, logConstructor)
	return rtm.syncTags(ctx, latest, desired)
}
