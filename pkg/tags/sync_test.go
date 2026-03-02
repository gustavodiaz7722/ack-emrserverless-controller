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

package tags_test

import (
	"testing"

	"github.com/aws-controllers-k8s/emrserverless-controller/pkg/tags"
	"github.com/stretchr/testify/assert"
)

func TestTagsChanged(t *testing.T) {
	tests := []struct {
		name        string
		desiredTags map[string]string
		latestTags  map[string]string
		expected    bool
	}{
		{
			name: "identical tags",
			desiredTags: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
			latestTags: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
			expected: false,
		},
		{
			name: "different tag values",
			desiredTags: map[string]string{
				"key1": "value1",
				"key2": "newvalue2",
			},
			latestTags: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
			expected: true,
		},
		{
			name: "different tag keys",
			desiredTags: map[string]string{
				"key1": "value1",
				"key3": "value3",
			},
			latestTags: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
			expected: true,
		},
		{
			name: "different number of tags",
			desiredTags: map[string]string{
				"key1": "value1",
				"key2": "value2",
				"key3": "value3",
			},
			latestTags: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
			expected: true,
		},
		{
			name:        "empty desired tags",
			desiredTags: map[string]string{},
			latestTags: map[string]string{
				"key1": "value1",
			},
			expected: true,
		},
		{
			name: "empty latest tags",
			desiredTags: map[string]string{
				"key1": "value1",
			},
			latestTags: map[string]string{},
			expected:   true,
		},
		{
			name:        "both empty",
			desiredTags: map[string]string{},
			latestTags:  map[string]string{},
			expected:    false,
		},
		{
			name:        "both nil",
			desiredTags: nil,
			latestTags:  nil,
			expected:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tags.TagsChanged(tt.desiredTags, tt.latestTags)
			assert.Equal(t, tt.expected, result)
		})
	}
}
