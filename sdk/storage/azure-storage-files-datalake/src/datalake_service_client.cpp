// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#include "azure/storage/files/datalake/datalake_service_client.hpp"

#include "azure/storage/files/datalake/datalake_file_system_client.hpp"
#include "private/datalake_utilities.hpp"
#include "private/package_version.hpp"

#include <azure/core/http/policies/policy.hpp>
#include <azure/storage/common/crypt.hpp>
#include <azure/storage/common/internal/constants.hpp>
#include <azure/storage/common/internal/storage_pipeline.hpp>
#include <azure/storage/common/internal/storage_switch_to_secondary_policy.hpp>
#include <azure/storage/common/storage_common.hpp>
#include <azure/storage/common/storage_credential.hpp>

namespace Azure { namespace Storage { namespace Files { namespace DataLake {

  DataLakeServiceClient DataLakeServiceClient::CreateFromConnectionString(
      const std::string& connectionString,
      const DataLakeClientOptions& options)
  {
    auto parsedConnectionString = _internal::ParseConnectionString(connectionString);
    auto serviceUrl = std::move(parsedConnectionString.DataLakeServiceUrl);

    if (parsedConnectionString.KeyCredential)
    {
      return DataLakeServiceClient(
          serviceUrl.GetAbsoluteUrl(), parsedConnectionString.KeyCredential, options);
    }
    else
    {
      return DataLakeServiceClient(serviceUrl.GetAbsoluteUrl(), options);
    }
  }

  DataLakeServiceClient::DataLakeServiceClient(
      const std::string& serviceUrl,
      std::shared_ptr<StorageSharedKeyCredential> credential,
      const DataLakeClientOptions& options)
      : m_serviceUrl(serviceUrl), m_blobServiceClient(
                                      _detail::GetBlobUrlFromUrl(serviceUrl),
                                      credential,
                                      _detail::GetBlobClientOptions(options))
  {
    m_clientConfiguration.ApiVersion
        = options.ApiVersion.empty() ? _detail::ApiVersion : options.ApiVersion;
    m_clientConfiguration.CustomerProvidedKey = options.CustomerProvidedKey;

    _internal::StorageHttpPipelineOptions serviceOptions;
    serviceOptions.SharedKeyCredential = credential;
    if (!options.SecondaryHostForRetryReads.empty())
    {
      serviceOptions.PrimaryHost = m_serviceUrl.GetHost();
      serviceOptions.SecondaryHostForRetryReads = options.SecondaryHostForRetryReads;
    }

    m_pipeline = _internal::BuildStorageHttpPipeline(
        options.ApiVersion,
        _internal::DatalakeServicePackageName,
        _detail::PackageVersion::ToString(),
        serviceOptions,
        options);
  }

  DataLakeServiceClient::DataLakeServiceClient(
      const std::string& serviceUrl,
      std::shared_ptr<Core::Credentials::TokenCredential> credential,
      const DataLakeClientOptions& options)
      : m_serviceUrl(serviceUrl), m_blobServiceClient(
                                      _detail::GetBlobUrlFromUrl(serviceUrl),
                                      credential,
                                      _detail::GetBlobClientOptions(options))
  {
    m_clientConfiguration.ApiVersion
        = options.ApiVersion.empty() ? _detail::ApiVersion : options.ApiVersion;
    m_clientConfiguration.TokenCredential = credential;
    m_clientConfiguration.CustomerProvidedKey = options.CustomerProvidedKey;

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
        _internal::DatalakeServicePackageName,
        _detail::PackageVersion::ToString(),
        serviceOptions,
        options);
  }

  DataLakeServiceClient::DataLakeServiceClient(
      const std::string& serviceUrl,
      const DataLakeClientOptions& options)
      : m_serviceUrl(serviceUrl), m_blobServiceClient(
                                      _detail::GetBlobUrlFromUrl(serviceUrl),
                                      _detail::GetBlobClientOptions(options))
  {
    m_clientConfiguration.ApiVersion
        = options.ApiVersion.empty() ? _detail::ApiVersion : options.ApiVersion;
    m_clientConfiguration.CustomerProvidedKey = options.CustomerProvidedKey;

    _internal::StorageHttpPipelineOptions serviceOptions;
    if (!options.SecondaryHostForRetryReads.empty())
    {
      serviceOptions.PrimaryHost = m_serviceUrl.GetHost();
      serviceOptions.SecondaryHostForRetryReads = options.SecondaryHostForRetryReads;
    }

    m_pipeline = _internal::BuildStorageHttpPipeline(
        options.ApiVersion,
        _internal::DatalakeServicePackageName,
        _detail::PackageVersion::ToString(),
        serviceOptions,
        options);
  }

  DataLakeFileSystemClient DataLakeServiceClient::GetFileSystemClient(
      const std::string& fileSystemName) const
  {
    auto builder = m_serviceUrl;
    builder.AppendPath(_internal::UrlEncodePath(fileSystemName));
    return DataLakeFileSystemClient(
        builder,
        m_blobServiceClient.GetBlobContainerClient(fileSystemName),
        m_pipeline,
        m_clientConfiguration);
  }

  ListFileSystemsPagedResponse DataLakeServiceClient::ListFileSystems(
      const ListFileSystemsOptions& options,
      const Azure::Core::Context& context) const
  {
    Blobs::ListBlobContainersOptions blobOptions;
    blobOptions.Include = options.Include;
    blobOptions.Prefix = options.Prefix;
    blobOptions.ContinuationToken = options.ContinuationToken;
    blobOptions.PageSizeHint = options.PageSizeHint;
    auto blobPagedResponse = m_blobServiceClient.ListBlobContainers(blobOptions, context);

    ListFileSystemsPagedResponse pagedResponse;

    pagedResponse.ServiceEndpoint = std::move(blobPagedResponse.ServiceEndpoint);
    pagedResponse.Prefix = std::move(blobPagedResponse.Prefix);
    for (auto& item : blobPagedResponse.BlobContainers)
    {
      Models::FileSystemItem fileSystem;
      fileSystem.Name = std::move(item.Name);
      fileSystem.Details.ETag = std::move(item.Details.ETag);
      fileSystem.Details.LastModified = std::move(item.Details.LastModified);
      fileSystem.Details.Metadata = std::move(item.Details.Metadata);
      if (item.Details.AccessType == Blobs::Models::PublicAccessType::BlobContainer)
      {
        fileSystem.Details.AccessType = Models::PublicAccessType::FileSystem;
      }
      else if (item.Details.AccessType == Blobs::Models::PublicAccessType::Blob)
      {
        fileSystem.Details.AccessType = Models::PublicAccessType::Path;
      }
      else if (item.Details.AccessType == Blobs::Models::PublicAccessType::None)
      {
        fileSystem.Details.AccessType = Models::PublicAccessType::None;
      }
      else
      {
        fileSystem.Details.AccessType
            = Models::PublicAccessType(item.Details.AccessType.ToString());
      }
      fileSystem.Details.HasImmutabilityPolicy = item.Details.HasImmutabilityPolicy;
      fileSystem.Details.HasLegalHold = item.Details.HasLegalHold;
      fileSystem.Details.LeaseDuration = std::move(item.Details.LeaseDuration);
      fileSystem.Details.LeaseState = std::move(item.Details.LeaseState);
      fileSystem.Details.LeaseStatus = std::move(item.Details.LeaseStatus);
      fileSystem.Details.DefaultEncryptionScope = std::move(item.Details.DefaultEncryptionScope);
      fileSystem.Details.PreventEncryptionScopeOverride
          = item.Details.PreventEncryptionScopeOverride;

      pagedResponse.FileSystems.emplace_back(std::move(fileSystem));
    }
    pagedResponse.m_dataLakeServiceClient = std::make_shared<DataLakeServiceClient>(*this);
    pagedResponse.m_operationOptions = options;
    pagedResponse.CurrentPageToken = std::move(blobPagedResponse.CurrentPageToken);
    pagedResponse.NextPageToken = std::move(blobPagedResponse.NextPageToken);
    pagedResponse.RawResponse = std::move(blobPagedResponse.RawResponse);

    return pagedResponse;
  }

}}}} // namespace Azure::Storage::Files::DataLake
