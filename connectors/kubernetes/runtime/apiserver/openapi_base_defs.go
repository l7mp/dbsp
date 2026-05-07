package apiserver

import (
	generatedopenapi "k8s.io/apiextensions-apiserver/pkg/generated/openapi"
	openapicommon "k8s.io/kube-openapi/pkg/common"
)

// BaseOpenAPIDefinitions supplies foundational OpenAPI model definitions.
func BaseOpenAPIDefinitions(ref openapicommon.ReferenceCallback) map[string]openapicommon.OpenAPIDefinition {
	return generatedopenapi.GetOpenAPIDefinitions(ref)
}
