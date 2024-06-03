// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#include "azure/storage/blobs/blob_service_client.hpp"

#include "azure/storage/blobs/blob_batch.hpp"
#include "private/package_version.hpp"

#include <azure/core/http/policies/policy.hpp>
#include <azure/storage/common/crypt.hpp>
#include <azure/storage/common/internal/constants.hpp>
#include <azure/storage/common/internal/storage_bearer_token_auth.hpp>
#include <azure/storage/common/internal/storage_pipeline.hpp>
#include <azure/storage/common/internal/storage_switch_to_secondary_policy.hpp>
#include <azure/storage/common/storage_common.hpp>

namespace Azure { namespace Storage { namespace Blobs {

  BlobServiceClient BlobServiceClient::CreateFromConnectionString(
      const std::string& connectionString,
      const BlobClientOptions& options)
  {
    auto parsedConnectionString = _internal::ParseConnectionString(connectionString);
    auto serviceUrl = std::move(parsedConnectionString.BlobServiceUrl);

    if (parsedConnectionString.KeyCredential)
    {
      return BlobServiceClient(
          serviceUrl.GetAbsoluteUrl(), parsedConnectionString.KeyCredential, options);
    }
    else
    {
      return BlobServiceClient(serviceUrl.GetAbsoluteUrl(), options);
    }
  }

  BlobServiceClient::BlobServiceClient(
      const std::string& serviceUrl,
      std::shared_ptr<StorageSharedKeyCredential> credential,
      const BlobClientOptions& options)
      : BlobServiceClient(serviceUrl, options)
  {
    _internal::StorageHttpPipelineOptions serviceOptions;
    serviceOptions.SharedKeyCredential = credential;
    if (!options.SecondaryHostForRetryReads.empty())
    {
      serviceOptions.PrimaryHost = m_serviceUrl.GetHost();
      serviceOptions.SecondaryHostForRetryReads = options.SecondaryHostForRetryReads;
    }

    m_pipeline = _internal::BuildStorageHttpPipeline(
        options.ApiVersion,
        _internal::BlobServicePackageName,
        _detail::PackageVersion::ToString(),
        serviceOptions,
        options);

    m_batchRequestPipeline = _detail::ConstructBatchRequestPipeline(nullptr, credential, options);
    m_batchSubrequestPipeline
        = _detail::ConstructBatchSubrequestPipeline(nullptr, credential, options);
  }

  BlobServiceClient::BlobServiceClient(
      const std::string& serviceUrl,
      std::shared_ptr<Core::Credentials::TokenCredential> credential,
      const BlobClientOptions& options)
      : BlobServiceClient(serviceUrl, options)
  {
    _internal::StorageHttpPipelineOptions serviceOptions;
    if (!options.SecondaryHostForRetryReads.empty())
    {
      serviceOptions.PrimaryHost = m_serviceUrl.GetHost();
      serviceOptions.SecondaryHostForRetryReads = options.SecondaryHostForRetryReads;
    }
    serviceOptions.TokenCredential = credential;
    serviceOptions.EnableTenantDiscovery = options.EnableTenantDiscovery;
    if (options.Audience.HasValue())
    {
      serviceOptions.TokenAudience = options.Audience.Value().ToString();
    }

    m_pipeline = _internal::BuildStorageHttpPipeline(
        options.ApiVersion,
        _internal::BlobServicePackageName,
        _detail::PackageVersion::ToString(),
        serviceOptions,
        options);

    {
      Azure::Core::Credentials::TokenRequestContext tokenContext;
      tokenContext.Scopes.emplace_back(
          options.Audience.HasValue()
              ? _internal::GetDefaultScopeForAudience(options.Audience.Value().ToString())
              : _internal::StorageScope);
      std::unique_ptr<Azure::Core::Http::Policies::HttpPolicy> tokenPolicy
          = std::make_unique<_internal::StorageBearerTokenAuthenticationPolicy>(
              credential, tokenContext, options.EnableTenantDiscovery);

      m_batchRequestPipeline
          = _detail::ConstructBatchRequestPipeline(tokenPolicy->Clone(), nullptr, options);
      m_batchSubrequestPipeline
          = _detail::ConstructBatchSubrequestPipeline(std::move(tokenPolicy), nullptr, options);
    }
  }

  BlobServiceClient::BlobServiceClient(
      const std::string& serviceUrl,
      const BlobClientOptions& options)
      : m_serviceUrl(serviceUrl), m_customerProvidedKey(options.CustomerProvidedKey),
        m_encryptionScope(options.EncryptionScope)
  {
    _internal::StorageHttpPipelineOptions serviceOptions;
    if (!options.SecondaryHostForRetryReads.empty())
    {
      serviceOptions.PrimaryHost = m_serviceUrl.GetHost();
      serviceOptions.SecondaryHostForRetryReads = options.SecondaryHostForRetryReads;
    }

    m_pipeline = _internal::BuildStorageHttpPipeline(
        options.ApiVersion,
        _internal::BlobServicePackageName,
        _detail::PackageVersion::ToString(),
        serviceOptions,
        options);

    m_batchRequestPipeline = _detail::ConstructBatchRequestPipeline(nullptr, nullptr, options);
    m_batchSubrequestPipeline
        = _detail::ConstructBatchSubrequestPipeline(nullptr, nullptr, options);
  }

  BlobContainerClient BlobServiceClient::GetBlobContainerClient(
      const std::string& blobContainerName) const
  {
    auto blobContainerUrl = m_serviceUrl;
    blobContainerUrl.AppendPath(_internal::UrlEncodePath(blobContainerName));

    BlobContainerClient blobContainerClient(blobContainerUrl.GetAbsoluteUrl());
    blobContainerClient.m_pipeline = m_pipeline;
    blobContainerClient.m_customerProvidedKey = m_customerProvidedKey;
    blobContainerClient.m_encryptionScope = m_encryptionScope;
    blobContainerClient.m_batchRequestPipeline = m_batchRequestPipeline;
    blobContainerClient.m_batchSubrequestPipeline = m_batchSubrequestPipeline;
    return blobContainerClient;
  }

  ListBlobContainersPagedResponse BlobServiceClient::ListBlobContainers(
      const ListBlobContainersOptions& options,
      const Azure::Core::Context& context) const
  {
    _detail::ServiceClient::ListServiceBlobContainersOptions protocolLayerOptions;
    protocolLayerOptions.Prefix = options.Prefix;
    protocolLayerOptions.Marker = options.ContinuationToken;
    protocolLayerOptions.MaxResults = options.PageSizeHint;
    protocolLayerOptions.Include = options.Include;
    auto response = _detail::ServiceClient::ListBlobContainers(
        *m_pipeline, m_serviceUrl, protocolLayerOptions, _internal::WithReplicaStatus(context));

    ListBlobContainersPagedResponse pagedResponse;
    pagedResponse.ServiceEndpoint = std::move(response.Value.ServiceEndpoint);
    pagedResponse.Prefix = std::move(response.Value.Prefix);
    pagedResponse.BlobContainers = std::move(response.Value.Items);
    pagedResponse.m_blobServiceClient = std::make_shared<BlobServiceClient>(*this);
    pagedResponse.m_operationOptions = options;
    pagedResponse.CurrentPageToken = options.ContinuationToken.ValueOr(std::string());
    pagedResponse.NextPageToken = response.Value.ContinuationToken;
    pagedResponse.RawResponse = std::move(response.RawResponse);

    return pagedResponse;
  }

  Azure::Response<Models::UserDelegationKey> BlobServiceClient::GetUserDelegationKey(
      const Azure::DateTime& expiresOn,
      const GetUserDelegationKeyOptions& options,
      const Azure::Core::Context& context) const
  {
    _detail::ServiceClient::GetServiceUserDelegationKeyOptions protocolLayerOptions;
    protocolLayerOptions.KeyInfo.Start = options.StartsOn.ToString(
        Azure::DateTime::DateFormat::Rfc3339, Azure::DateTime::TimeFractionFormat::Truncate);
    protocolLayerOptions.KeyInfo.Expiry = expiresOn.ToString(
        Azure::DateTime::DateFormat::Rfc3339, Azure::DateTime::TimeFractionFormat::Truncate);
    return _detail::ServiceClient::GetUserDelegationKey(
        *m_pipeline, m_serviceUrl, protocolLayerOptions, _internal::WithReplicaStatus(context));
  }

  Azure::Response<Models::SetServicePropertiesResult> BlobServiceClient::SetProperties(
      Models::BlobServiceProperties properties,
      const SetServicePropertiesOptions& options,
      const Azure::Core::Context& context) const
  {
    (void)options;
    _detail::ServiceClient::SetServicePropertiesOptions protocolLayerOptions;
    protocolLayerOptions.BlobServiceProperties = std::move(properties);
    return _detail::ServiceClient::SetProperties(
        *m_pipeline, m_serviceUrl, protocolLayerOptions, context);
  }

  Azure::Response<Models::BlobServiceProperties> BlobServiceClient::GetProperties(
      const GetServicePropertiesOptions& options,
      const Azure::Core::Context& context) const
  {
    (void)options;
    _detail::ServiceClient::GetServicePropertiesOptions protocolLayerOptions;
    return _detail::ServiceClient::GetProperties(
        *m_pipeline, m_serviceUrl, protocolLayerOptions, _internal::WithReplicaStatus(context));
  }

  Azure::Response<Models::AccountInfo> BlobServiceClient::GetAccountInfo(
      const GetAccountInfoOptions& options,
      const Azure::Core::Context& context) const
  {
    (void)options;
    _detail::ServiceClient::GetServiceAccountInfoOptions protocolLayerOptions;
    return _detail::ServiceClient::GetAccountInfo(
        *m_pipeline, m_serviceUrl, protocolLayerOptions, _internal::WithReplicaStatus(context));
  }

  Azure::Response<Models::ServiceStatistics> BlobServiceClient::GetStatistics(
      const GetBlobServiceStatisticsOptions& options,
      const Azure::Core::Context& context) const
  {
    (void)options;
    _detail::ServiceClient::GetServiceStatisticsOptions protocolLayerOptions;
    return _detail::ServiceClient::GetStatistics(
        *m_pipeline, m_serviceUrl, protocolLayerOptions, context);
  }

  FindBlobsByTagsPagedResponse BlobServiceClient::FindBlobsByTags(
      const std::string& tagFilterSqlExpression,
      const FindBlobsByTagsOptions& options,
      const Azure::Core::Context& context) const
  {
    _detail::ServiceClient::FindServiceBlobsByTagsOptions protocolLayerOptions;
    protocolLayerOptions.Where = tagFilterSqlExpression;
    protocolLayerOptions.Marker = options.ContinuationToken;
    protocolLayerOptions.MaxResults = options.PageSizeHint;
    auto response = _detail::ServiceClient::FindBlobsByTags(
        *m_pipeline, m_serviceUrl, protocolLayerOptions, _internal::WithReplicaStatus(context));

    FindBlobsByTagsPagedResponse pagedResponse;
    pagedResponse.ServiceEndpoint = std::move(response.Value.ServiceEndpoint);
    pagedResponse.TaggedBlobs = std::move(response.Value.Items);
    pagedResponse.m_blobServiceClient = std::make_shared<BlobServiceClient>(*this);
    pagedResponse.m_operationOptions = options;
    pagedResponse.m_tagFilterSqlExpression = tagFilterSqlExpression;
    pagedResponse.CurrentPageToken = options.ContinuationToken.ValueOr(std::string());
    pagedResponse.NextPageToken = response.Value.ContinuationToken;
    pagedResponse.RawResponse = std::move(response.RawResponse);

    return pagedResponse;
  }

  Azure::Response<BlobContainerClient> BlobServiceClient::CreateBlobContainer(
      const std::string& blobContainerName,
      const CreateBlobContainerOptions& options,
      const Azure::Core::Context& context) const
  {
    auto blobContainerClient = GetBlobContainerClient(blobContainerName);
    auto response = blobContainerClient.Create(options, context);
    return Azure::Response<BlobContainerClient>(
        std::move(blobContainerClient), std::move(response.RawResponse));
  }

  Azure::Response<Models::DeleteBlobContainerResult> BlobServiceClient::DeleteBlobContainer(
      const std::string& blobContainerName,
      const DeleteBlobContainerOptions& options,
      const Azure::Core::Context& context) const
  {
    auto blobContainerClient = GetBlobContainerClient(blobContainerName);
    return blobContainerClient.Delete(options, context);
  }

  Azure::Response<BlobContainerClient> BlobServiceClient::UndeleteBlobContainer(
      const std::string& deletedBlobContainerName,
      const std::string& deletedBlobContainerVersion,
      const UndeleteBlobContainerOptions& options,
      const Azure::Core::Context& context) const
  {
    (void)options;

    auto blobContainerClient = GetBlobContainerClient(deletedBlobContainerName);

    _detail::BlobContainerClient::UndeleteBlobContainerOptions protocolLayerOptions;
    protocolLayerOptions.DeletedContainerName = deletedBlobContainerName;
    protocolLayerOptions.DeletedContainerVersion = deletedBlobContainerVersion;
    auto response = _detail::BlobContainerClient::Undelete(
        *m_pipeline, Azure::Core::Url(blobContainerClient.GetUrl()), protocolLayerOptions, context);

    return Azure::Response<BlobContainerClient>(
        std::move(blobContainerClient), std::move(response.RawResponse));
  }

  Azure::Response<BlobContainerClient> BlobServiceClient::RenameBlobContainer(
      const std::string& sourceBlobContainerName,
      const std::string& destinationBlobContainerName,
      const RenameBlobContainerOptions& options,
      const Azure::Core::Context& context) const
  {
    auto blobContainerClient = GetBlobContainerClient(destinationBlobContainerName);

    _detail::BlobContainerClient::RenameBlobContainerOptions protocolLayerOptions;
    protocolLayerOptions.SourceContainerName = sourceBlobContainerName;
    protocolLayerOptions.SourceLeaseId = options.SourceAccessConditions.LeaseId;
    auto response = _detail::BlobContainerClient::Rename(
        *m_pipeline, Azure::Core::Url(blobContainerClient.GetUrl()), protocolLayerOptions, context);

    return Azure::Response<BlobContainerClient>(
        std::move(blobContainerClient), std::move(response.RawResponse));
  }

  BlobServiceBatch BlobServiceClient::CreateBatch() const { return BlobServiceBatch(*this); }

  Response<Models::SubmitBlobBatchResult> BlobServiceClient::SubmitBatch(
      const BlobServiceBatch& batch,
      const SubmitBlobBatchOptions& options,
      const Core::Context& context) const
  {
    (void)options;

    _detail::ServiceClient::SubmitServiceBatchOptions protocolLayerOptions;
    _detail::StringBodyStream bodyStream(std::string{});
    auto response = _detail::ServiceClient::SubmitBatch(
        *m_batchRequestPipeline,
        m_serviceUrl,
        bodyStream,
        protocolLayerOptions,
        context.WithValue(_detail::s_serviceBatchKey, &batch));
    return Azure::Response<Models::SubmitBlobBatchResult>(
        Models::SubmitBlobBatchResult(), std::move(response.RawResponse));
  }

}}} // namespace Azure::Storage::Blobs
