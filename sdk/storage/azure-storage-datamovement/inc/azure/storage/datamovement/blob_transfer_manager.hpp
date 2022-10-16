// Copyright (c) Microsoft Corporation. All rights reserved.
// SPDX-License-Identifier: MIT

#pragma once

#include <string>

#include "azure/storage/datamovement/blob_folder.hpp"
#include "azure/storage/datamovement/datamovement_options.hpp"
#include "azure/storage/datamovement/storage_transfer_manager.hpp"

namespace Azure { namespace Storage { namespace Blobs {

  class BlobTransferManager final : public StorageTransferManager {
  public:
    explicit BlobTransferManager(
        const StorageTransferManagerOptions& options = StorageTransferManagerOptions())
        : StorageTransferManager(options)
    {
    }

    JobProperties ScheduleUpload(
        const std::string& sourceLocalPath,
        const BlobClient& destinationBlob,
        const ScheduleUploadBlobOptions& options = ScheduleUploadBlobOptions());

    JobProperties ScheduleUploadDirectory(
        const std::string& sourceLocalPath,
        const BlobFolder& destinationBlobFolder,
        const ScheduleUploadBlobOptions& options = ScheduleUploadBlobOptions());

    JobProperties ScheduleDownload(
        const BlobClient& sourceBlob,
        const std::string& destinationLocalPath,
        const ScheduleDownloadBlobOptions& options = ScheduleDownloadBlobOptions());

    JobProperties ScheduleDownloadDirectory(
        const BlobFolder& sourceBlobFolder,
        const std::string& destinationLocalPath,
        const ScheduleDownloadBlobOptions& options = ScheduleDownloadBlobOptions());

    JobProperties ScheduleCopy(
        const BlobClient& sourceBlob,
        const BlobClient& destinationBlob,
        const ScheduleCopyBlobOptions& options = ScheduleCopyBlobOptions());

    JobProperties ScheduleCopyDirectory(
        const BlobFolder& sourceBlobFolder,
        const BlobFolder& destinationBlobFolder,
        const ScheduleCopyBlobOptions& options = ScheduleCopyBlobOptions());
  };

}}} // namespace Azure::Storage::Blobs
