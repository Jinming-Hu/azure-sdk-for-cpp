// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#include "azure/storage/common/internal/storage_pipeline.hpp"

#include "azure/storage/common/internal/shared_key_policy.hpp"
#include "azure/storage/common/internal/storage_bearer_token_auth.hpp"
#include "azure/storage/common/internal/storage_per_retry_policy.hpp"
#include "azure/storage/common/internal/storage_retry_policy.hpp"
#include "azure/storage/common/internal/storage_service_version_policy.hpp"
#include "azure/storage/common/internal/storage_switch_to_secondary_policy.hpp"

namespace Azure { namespace Storage { namespace _internal {

  std::shared_ptr<Azure::Core::Http::_internal::HttpPipeline> BuildStorageHttpPipeline(
      const std::string& apiVersion,
      const std::string& telemetryPackageName,
      const std::string& telemetryPackageVersion,
      StorageHttpPipelineOptions& serviceOptions,
      const Azure::Core::_internal::ClientOptions& clientOptions)
  {
    std::vector<std::unique_ptr<Azure::Core::Http::Policies::HttpPolicy>> policies;

    // service-specific per call policies
    policies.push_back(std::make_unique<_internal::StorageServiceVersionPolicy>(apiVersion));

    // Request Id
    policies.push_back(std::make_unique<Azure::Core::Http::Policies::_internal::RequestIdPolicy>());

    // Telemetry (user-agent header)
    policies.push_back(std::make_unique<Azure::Core::Http::Policies::_internal::TelemetryPolicy>(
        telemetryPackageName, telemetryPackageVersion, clientOptions.Telemetry));

    // client-options per call policies.
    for (auto& policy : clientOptions.PerOperationPolicies)
    {
      policies.push_back(policy->Clone());
    }

    // Retry policy
    policies.push_back(std::make_unique<StorageRetryPolicy>(clientOptions.Retry));

    if (serviceOptions.SecondaryHostForRetryReads.HasValue()
        && !serviceOptions.SecondaryHostForRetryReads.Value().empty())
    {
      auto policy = std::make_unique<_internal::StorageSwitchToSecondaryPolicy>(
          serviceOptions.PrimaryHost.Value(), serviceOptions.SecondaryHostForRetryReads.Value());
      policies.push_back(std::move(policy));
    }

    // service-specific per retry policies.
    if (serviceOptions.TokenCredential)
    {
      Azure::Core::Credentials::TokenRequestContext tokenContext;
      tokenContext.Scopes.emplace_back(
          serviceOptions.TokenAudience.HasValue()
              ? _internal::GetDefaultScopeForAudience(serviceOptions.TokenAudience.Value())
              : _internal::StorageScope);
      auto policy = serviceOptions.EnableTenantDiscovery.HasValue()
          ? std::make_unique<_internal::StorageBearerTokenAuthenticationPolicy>(
              serviceOptions.TokenCredential,
              tokenContext,
              serviceOptions.EnableTenantDiscovery.Value())
          : std::make_unique<
              Azure::Core::Http::Policies::_internal::BearerTokenAuthenticationPolicy>(
              serviceOptions.TokenCredential, tokenContext);
      policies.emplace_back(std::move(policy));
    }
    policies.push_back(std::make_unique<StoragePerRetryPolicy>());

    // client options per retry policies.
    for (auto& policy : clientOptions.PerRetryPolicies)
    {
      policies.push_back(policy->Clone());
    }

    if (serviceOptions.SharedKeyCredential)
    {
      auto policy = std::make_unique<SharedKeyPolicy>(serviceOptions.SharedKeyCredential);
      policies.push_back(std::move(policy));
    }

    // Policies after here cannot modify the request anymore

    // Add a request activity policy which will generate distributed traces for the pipeline.
    Azure::Core::Http::_internal::HttpSanitizer httpSanitizer(
        clientOptions.Log.AllowedHttpQueryParameters, clientOptions.Log.AllowedHttpHeaders);
    policies.push_back(
        std::make_unique<Azure::Core::Http::Policies::_internal::RequestActivityPolicy>(
            httpSanitizer));

    // logging - won't update request
    policies.push_back(
        std::make_unique<Azure::Core::Http::Policies::_internal::LogPolicy>(clientOptions.Log));

    // transport
    policies.push_back(std::make_unique<Azure::Core::Http::Policies::_internal::TransportPolicy>(
        clientOptions.Transport));

    return std::make_shared<Azure::Core::Http::_internal::HttpPipeline>(policies);
  }

}}} // namespace Azure::Storage::_internal
