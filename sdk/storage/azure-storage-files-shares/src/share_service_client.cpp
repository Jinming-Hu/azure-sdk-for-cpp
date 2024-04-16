// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#include "azure/storage/files/shares/share_service_client.hpp"

#include "azure/storage/files/shares/share_client.hpp"
#include "private/package_version.hpp"

#include <azure/core/credentials/credentials.hpp>
#include <azure/core/http/policies/policy.hpp>
#include <azure/storage/common/crypt.hpp>
#include <azure/storage/common/internal/constants.hpp>
#include <azure/storage/common/internal/storage_pipeline.hpp>
#include <azure/storage/common/storage_common.hpp>
#include <azure/storage/common/storage_credential.hpp>

namespace Azure { namespace Storage { namespace Files { namespace Shares {
  ShareServiceClient ShareServiceClient::CreateFromConnectionString(
      const std::string& connectionString,
      const ShareClientOptions& options)
  {
    auto parsedConnectionString = _internal::ParseConnectionString(connectionString);
    auto serviceUrl = std::move(parsedConnectionString.FileServiceUrl);

    if (parsedConnectionString.KeyCredential)
    {
      return ShareServiceClient(
          serviceUrl.GetAbsoluteUrl(), parsedConnectionString.KeyCredential, options);
    }
    else
    {
      return ShareServiceClient(serviceUrl.GetAbsoluteUrl(), options);
    }
  }

  ShareServiceClient::ShareServiceClient(
      const std::string& serviceUrl,
      std::shared_ptr<StorageSharedKeyCredential> credential,
      const ShareClientOptions& options)
      : m_serviceUrl(serviceUrl), m_allowTrailingDot(options.AllowTrailingDot),
        m_allowSourceTrailingDot(options.AllowSourceTrailingDot),
        m_shareTokenIntent(options.ShareTokenIntent)
  {
    _internal::StorageHttpPipelineOptions serviceOptions;
    serviceOptions.SharedKeyCredential = credential;

    m_pipeline = _internal::BuildStorageHttpPipeline(
        options.ApiVersion,
        _internal::FileServicePackageName,
        _detail::PackageVersion::ToString(),
        serviceOptions,
        options);
  }

  ShareServiceClient::ShareServiceClient(
      const std::string& serviceUrl,
      std::shared_ptr<Core::Credentials::TokenCredential> credential,
      const ShareClientOptions& options)
      : m_serviceUrl(serviceUrl), m_allowTrailingDot(options.AllowTrailingDot),
        m_allowSourceTrailingDot(options.AllowSourceTrailingDot),
        m_shareTokenIntent(options.ShareTokenIntent)
  {
    _internal::StorageHttpPipelineOptions serviceOptions;
    serviceOptions.TokenCredential = credential;
    if (options.Audience.HasValue())
    {
      serviceOptions.TokenAudience = options.Audience.Value().ToString();
    }

    m_pipeline = _internal::BuildStorageHttpPipeline(
        options.ApiVersion,
        _internal::FileServicePackageName,
        _detail::PackageVersion::ToString(),
        serviceOptions,
        options);
  }

  ShareServiceClient::ShareServiceClient(
      const std::string& serviceUrl,
      const ShareClientOptions& options)
      : m_serviceUrl(serviceUrl), m_allowTrailingDot(options.AllowTrailingDot),
        m_allowSourceTrailingDot(options.AllowSourceTrailingDot),
        m_shareTokenIntent(options.ShareTokenIntent)
  {
    _internal::StorageHttpPipelineOptions serviceOptions;

    m_pipeline = _internal::BuildStorageHttpPipeline(
        options.ApiVersion,
        _internal::FileServicePackageName,
        _detail::PackageVersion::ToString(),
        serviceOptions,
        options);
  }

  ShareClient ShareServiceClient::GetShareClient(const std::string& shareName) const
  {
    auto builder = m_serviceUrl;
    builder.AppendPath(_internal::UrlEncodePath(shareName));
    ShareClient shareClient(builder, m_pipeline);
    shareClient.m_allowTrailingDot = m_allowTrailingDot;
    shareClient.m_allowSourceTrailingDot = m_allowSourceTrailingDot;
    shareClient.m_shareTokenIntent = m_shareTokenIntent;
    return shareClient;
  }

  ListSharesPagedResponse ShareServiceClient::ListShares(
      const ListSharesOptions& options,
      const Azure::Core::Context& context) const
  {
    auto protocolLayerOptions = _detail::ServiceClient::ListServiceSharesSegmentOptions();
    protocolLayerOptions.Include = options.ListSharesIncludeFlags;
    protocolLayerOptions.Marker = options.ContinuationToken;
    protocolLayerOptions.MaxResults = options.PageSizeHint;
    protocolLayerOptions.Prefix = options.Prefix;
    auto response = _detail::ServiceClient::ListSharesSegment(
        *m_pipeline, m_serviceUrl, protocolLayerOptions, context);

    ListSharesPagedResponse pagedResponse;
    pagedResponse.ServiceEndpoint = std::move(response.Value.ServiceEndpoint);
    pagedResponse.Prefix = response.Value.Prefix.ValueOr(std::string());
    pagedResponse.Shares = std::move(response.Value.ShareItems);
    pagedResponse.m_shareServiceClient = std::make_shared<ShareServiceClient>(*this);
    pagedResponse.m_operationOptions = options;
    pagedResponse.CurrentPageToken = options.ContinuationToken.ValueOr(std::string());
    if (!response.Value.NextMarker.empty())
    {
      pagedResponse.NextPageToken = response.Value.NextMarker;
    }
    pagedResponse.RawResponse = std::move(response.RawResponse);

    return pagedResponse;
  }

  Azure::Response<Models::SetServicePropertiesResult> ShareServiceClient::SetProperties(
      Models::ShareServiceProperties properties,
      const SetServicePropertiesOptions& options,
      const Azure::Core::Context& context) const
  {
    (void)options;
    auto protocolLayerOptions = _detail::ServiceClient::SetServicePropertiesOptions();
    protocolLayerOptions.ShareServiceProperties = std::move(properties);
    return _detail::ServiceClient::SetProperties(
        *m_pipeline, m_serviceUrl, protocolLayerOptions, context);
  }

  Azure::Response<Models::ShareServiceProperties> ShareServiceClient::GetProperties(
      const GetServicePropertiesOptions& options,
      const Azure::Core::Context& context) const
  {
    (void)options;
    auto protocolLayerOptions = _detail::ServiceClient::GetServicePropertiesOptions();
    auto result = _detail::ServiceClient::GetProperties(
        *m_pipeline, m_serviceUrl, protocolLayerOptions, context);
    Models::ShareServiceProperties ret;
    ret.Cors = std::move(result.Value.Cors);
    ret.HourMetrics = std::move(result.Value.HourMetrics);
    ret.MinuteMetrics = std::move(result.Value.MinuteMetrics);
    ret.Protocol = std::move(result.Value.Protocol);
    return Azure::Response<Models::ShareServiceProperties>(
        std::move(ret), std::move(result.RawResponse));
  }

}}}} // namespace Azure::Storage::Files::Shares
