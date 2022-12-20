// Copyright (c) Microsoft Corporation. All rights reserved.
// SPDX-License-Identifier: MIT

#include <vector>

#include <azure/core/cryptography/hash.hpp>
#include <azure/core/nullable.hpp>

#include "azure/storage/common/storage_common.hpp"

namespace Azure { namespace Storage {

  /**
   * @brief Algorithm for generating a checksum to be used for verifying transferred data.
   */
  enum class StorageChecksumAlgorithm
  {
    /**
     * Recommended. Allow the library to choose an algorithm. Different library versions may
     */
    Auto = 0,
    /**
     * No selected algorithm. Do not calculate or request checksums.
     */
    None = 1,
    /**
     * Standard MD5 hash algorithm.
     */
    Md5,
    /**
     * Azure Storage custom 64 bit CRC.
     */
    StorageCrc64,
  };

  /**
   * @brief Options for additional content integrity checks on upload.
   */
  struct UploadTransferValidationOptions
  {
    /**
     * Checksum algorithm to use.
     */
    StorageChecksumAlgorithm ChecksumAlgorithm = StorageChecksumAlgorithm::None;

    /**
     * Optional. Can only be used on specific operations and not at the client level. An existing
     * checksum of the data to be uploaded. Not all upload APIs can use this value, and will throw
     * if one is provided. Please check documentation on specific APIs for whether this can be used.
     */
    std::vector<uint8_t> PrecalculatedChecksum;
  };

  /**
   * @brief Options for additional content integrity checks on download.
   */
  struct DownloadTransferValidationOptions
  {
    /**
     * Checksum algorithm to use.
     */
    StorageChecksumAlgorithm ChecksumAlgorithm = StorageChecksumAlgorithm::None;

    /**
     *
     */
    bool AutoValidateChecksum = true;
  };

  /**
   * @brief Options for additional content integrity checks on data transfer.
   */
  struct TransferValidationOptions
  {
    /**
     * Options for uploading operations.
     */
    UploadTransferValidationOptions Upload;

    /**
     * Options for downloading operations.
     */
    DownloadTransferValidationOptions Download;
  };

  namespace _internal {
    class VectorBodyStream final : public Core::IO::BodyStream {
    public:
      explicit VectorBodyStream(std::vector<uint8_t>&& data) : m_data(std::move(data)) {}
      int64_t Length() const override { return m_data.size(); }
      void Rewind() override { m_offset = 0; }

    private:
      size_t OnRead(uint8_t* buffer, size_t count, Core::Context const& context) override;

    private:
      const std::vector<uint8_t> m_data;
      size_t m_offset = 0;
    };

    class ChecksumBodyStream final : public Core::IO::BodyStream {
    public:
      explicit ChecksumBodyStream(
          std::unique_ptr<Core::IO::BodyStream> inner,
          ContentHash checksum);

      int64_t Length() const override { return this->m_inner->Length(); }
      void Verify() const;

    private:
      size_t OnRead(uint8_t* buffer, size_t count, Core::Context const& context) override;

    private:
      std::unique_ptr<Core::IO::BodyStream> m_inner;
      ContentHash m_checksum;
      std::unique_ptr<Core::Cryptography::Hash> m_checksumCalculator;
    };

    StorageChecksumAlgorithm GetChecksumAlgorithmForDownloadOperation(
        const TransferValidationOptions& clientLevelOptions,
        const DownloadTransferValidationOptions& operationLevelOptions,
        const Nullable<HashAlgorithm>& legacyOperationLevelOptions);

    std::pair<StorageChecksumAlgorithm, std::vector<uint8_t>> GetChecksumForUploadOperation(
        const TransferValidationOptions& clientLevelOptions,
        const UploadTransferValidationOptions& operationLevelOptions,
        const Nullable<ContentHash>& legacyOperationLevelOptions,
        Core::IO::BodyStream& content,
        const Core::Context& context);

    std::pair<StorageChecksumAlgorithm, std::vector<uint8_t>> GetChecksumForUploadOperation(
        const TransferValidationOptions& clientLevelOptions,
        const UploadTransferValidationOptions& operationLevelOptions,
        const Nullable<ContentHash>& legacyOperationLevelOptions,
        const uint8_t* content,
        size_t contentSize);
  } // namespace _internal

}} // namespace Azure::Storage
