{{/* 
  This hook is called after setting the output fields from the GetApplication API call.
  It retrieves the tags for the resource using ListTagsForResource and sets them in the Spec.Tags field.
  Note: GetApplication returns tags, but we use ListTagsForResource for consistency with tag sync.
*/}}
if ko.Status.ACKResourceMetadata != nil && ko.Status.ACKResourceMetadata.ARN != nil {
    tags := rm.getTags(ctx, string(*ko.Status.ACKResourceMetadata.ARN))
    if tags != nil {
        ko.Spec.Tags = tags
    }
}
