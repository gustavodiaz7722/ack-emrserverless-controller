{{/* 
  This hook is called after setting the output fields from the CreateApplication API call.
  It marks resource as not synced if tags were specified, since we need to verify tags were applied.
*/}}
if ko.Spec.Tags != nil {
    ackcondition.SetSynced(&resource{ko}, corev1.ConditionFalse, nil, nil)
}
