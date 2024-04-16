// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#pragma once

#include "../storage_credential.hpp"

#include <azure/core/internal/http/pipeline.hpp>

namespace Azure { namespace Storage { namespace _internal {

  struct StorageHttpPipelineOptions
  {
    std::shared_ptr<StorageSharedKeyCredential> SharedKeyCredential;
    std::shared_ptr<Core::Credentials::TokenCredential> TokenCredential;
    Azure::Nullable<std::string> TokenAudience;
    Azure::Nullable<bool> EnableTenantDiscovery;
    Azure::Nullable<std::string> PrimaryHost;
    Azure::Nullable<std::string> SecondaryHostForRetryReads;
  };

  std::shared_ptr<Azure::Core::Http::_internal::HttpPipeline> BuildStorageHttpPipeline(
      const std::string& apiVersion,
      const std::string& telemetryPackageName,
      const std::string& telemetryPackageVersion,
      StorageHttpPipelineOptions& serviceOptions,
      const Azure::Core::_internal::ClientOptions& clientOptions);

}}} // namespace Azure::Storage::_internal
