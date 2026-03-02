{{/* 
  This hook is called before building the UpdateApplication API request.
  It handles tag synchronization when tags are changed.
  Note: UpdateApplication API does not support tags, so we use TagResource/UntagResource.
*/}}
if delta.DifferentAt("Spec.Tags") {
    err := rm.syncTags(
        ctx,
        latest,
        desired,
    )
    if err != nil {
        return nil, err
    }
}
if !delta.DifferentExcept("Spec.Tags") {
    return desired, nil
}
