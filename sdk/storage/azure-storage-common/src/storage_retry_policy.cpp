// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#include "azure/storage/common/internal/storage_retry_policy.hpp"

#include "azure/core/internal/diagnostics/log.hpp"

namespace Azure { namespace Storage { namespace _internal {

  bool StorageRetryPolicy::ShouldRetry(
      const std::unique_ptr<Core::Http::RawResponse>& response,
      const Core::Http::Policies::RetryOptions& retryOptions) const
  {
    (void)retryOptions;
    if (static_cast<std::underlying_type_t<Core::Http::HttpStatusCode>>(response->GetStatusCode())
        >= 400)
    {
      const auto& headers = response->GetHeaders();
      auto ite = headers.find("x-ms-copy-source-error-code");
      if (ite != headers.end())
      {
        const auto& errorCode = ite->second;
        const bool shouldRetry = errorCode == "InternalError" || errorCode == "OperationTimedOut"
            || errorCode == "ServerBusy";

        if (Azure::Core::Diagnostics::_internal::Log::ShouldWrite(
                Azure::Core::Diagnostics::Logger::Level::Informational))
        {
          Azure::Core::Diagnostics::_internal::Log::Write(
              Azure::Core::Diagnostics::Logger::Level::Informational,
              std::string("x-ms-copy-source-error-code ") + errorCode
                  + (shouldRetry ? " will be retried" : " won't be retried"));
        }
        if (shouldRetry)
        {
          return true;
        }
      }
    }
    return false;
  }

}}} // namespace Azure::Storage::_internal
