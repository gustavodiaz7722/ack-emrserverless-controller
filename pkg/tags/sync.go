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

package tags

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/emrserverless"

	acktypes "github.com/aws-controllers-k8s/runtime/pkg/types"
)

// TagManager provides methods for working with AWS resource tags
type TagManager struct {
	client *emrserverless.Client
	// logConstructor contains a method that can produce a logger for a
	// resource manager from a supplied context.
	logConstructor func(context.Context) acktypes.Logger
}

// NewTagManager creates a new TagManager instance
func NewTagManager(
	cfg aws.Config,
	logConstructor func(context.Context) acktypes.Logger,
) *TagManager {
	return &TagManager{
		client:         emrserverless.NewFromConfig(cfg),
		logConstructor: logConstructor,
	}
}

// GetTags returns the tags for a given resource ARN
func (tm *TagManager) GetTags(
	ctx context.Context,
	resourceARN string,
) (map[string]string, error) {
	logger := tm.logConstructor(ctx)
	logger.Debug("getting tags for resource", "resource_arn", resourceARN)

	input := &emrserverless.ListTagsForResourceInput{
		ResourceArn: aws.String(resourceARN),
	}

	resp, err := tm.client.ListTagsForResource(ctx, input)
	if err != nil {
		return nil, err
	}

	return resp.Tags, nil
}

// SyncTags synchronizes tags between the supplied desired and latest resources
func (tm *TagManager) SyncTags(
	ctx context.Context,
	resourceARN string,
	desiredTags map[string]string,
	latestTags map[string]string,
) (bool, error) {
	logger := tm.logConstructor(ctx)
	logger.Debug("syncing tags for resource", "resource_arn", resourceARN)

	// If there are no differences, return early
	if !TagsChanged(desiredTags, latestTags) {
		return false, nil
	}

	// Determine which tags to add and which to remove
	tagsToAdd := map[string]string{}
	tagKeysToRemove := []string{}

	// Find tags to add or update
	for key, desiredValue := range desiredTags {
		latestValue, exists := latestTags[key]
		if !exists || latestValue != desiredValue {
			tagsToAdd[key] = desiredValue
		}
	}

	// Find tags to remove
	for key := range latestTags {
		_, exists := desiredTags[key]
		if !exists {
			tagKeysToRemove = append(tagKeysToRemove, key)
		}
	}

	// Add new or updated tags
	if len(tagsToAdd) > 0 {
		_, err := tm.client.TagResource(
			ctx,
			&emrserverless.TagResourceInput{
				ResourceArn: aws.String(resourceARN),
				Tags:        tagsToAdd,
			},
		)
		if err != nil {
			return false, err
		}
	}

	// Remove tags
	if len(tagKeysToRemove) > 0 {
		_, err := tm.client.UntagResource(
			ctx,
			&emrserverless.UntagResourceInput{
				ResourceArn: aws.String(resourceARN),
				TagKeys:     tagKeysToRemove,
			},
		)
		if err != nil {
			return false, err
		}
	}

	return true, nil
}

// TagsChanged returns true if there are differences between two tag maps
func TagsChanged(
	desiredTags map[string]string,
	latestTags map[string]string,
) bool {
	if len(desiredTags) != len(latestTags) {
		return true
	}

	// Check if all desired tags exist with the same values
	for key, desiredValue := range desiredTags {
		latestValue, exists := latestTags[key]
		if !exists || latestValue != desiredValue {
			return true
		}
	}

	// Check if all latest tags exist in desired tags
	for key := range latestTags {
		_, exists := desiredTags[key]
		if !exists {
			return true
		}
	}

	return false
}
