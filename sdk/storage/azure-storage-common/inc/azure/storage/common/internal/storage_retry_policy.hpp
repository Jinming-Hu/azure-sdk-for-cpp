// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#pragma once

#include <azure/core/http/policies/policy.hpp>

namespace Azure { namespace Storage { namespace _internal {

  class StorageRetryPolicy final : public Core::Http::Policies::_internal::RetryPolicy {
  public:
    explicit StorageRetryPolicy(Core::Http::Policies::RetryOptions options)
        : RetryPolicy(std::move(options))
    {
    }
    ~StorageRetryPolicy() override {}

    std::unique_ptr<HttpPolicy> Clone() const override
    {
      return std::make_unique<StorageRetryPolicy>(*this);
    }

  protected:
    bool ShouldRetry(
        const std::unique_ptr<Core::Http::RawResponse>& response,
        const Core::Http::Policies::RetryOptions& retryOptions) const override;
  };

}}} // namespace Azure::Storage::_internal
