// Copyright (c) Microsoft Corporation. All rights reserved.
// SPDX-License-Identifier: MIT

#include "azure/storage/common/transfer_validation.hpp"

#include "azure/storage/common/crypt.hpp"

namespace Azure { namespace Storage { namespace _internal {

  size_t VectorBodyStream::OnRead(uint8_t* buffer, size_t count, Core::Context const& context)

  {
    (void)context;
    size_t copyLength = std::min(count, m_data.size() - this->m_offset);
    std::memcpy(buffer, this->m_data.data() + m_offset, static_cast<size_t>(copyLength));
    m_offset += copyLength;
    return copyLength;
  }

  ChecksumBodyStream::ChecksumBodyStream(
      std::unique_ptr<Azure::Core::IO::BodyStream> inner,
      ContentHash checksum)
      : m_inner(std::move(inner)), m_checksum(std::move(checksum))
  {
    if (checksum.Algorithm == HashAlgorithm::Md5)
    {
      m_checksumCalculator = std::make_unique<Core::Cryptography::Md5Hash>();
    }
    else if (checksum.Algorithm == HashAlgorithm::Crc64)
    {
      m_checksumCalculator = std::make_unique<Crc64Hash>();
    }
    else
    {
      AZURE_UNREACHABLE_CODE();
    }
  }

  void ChecksumBodyStream::Verify() const
  {
    if (m_checksum.Value != m_checksumCalculator->Final())
    {
      throw Azure::Core::RequestFailedException("Checksum mismatch.");
    }
  }

  size_t ChecksumBodyStream::OnRead(uint8_t* buffer, size_t count, Core::Context const& context)
  {
    size_t bytesRead = m_inner->Read(buffer, count, context);
    if (m_checksumCalculator)
    {
      m_checksumCalculator->Append(buffer, bytesRead);
    }
    return bytesRead;
  }

  StorageChecksumAlgorithm GetChecksumAlgorithmForDownloadOperation(
      const TransferValidationOptions& clientLevelOptions,
      const DownloadTransferValidationOptions& operationLevelOptions,
      const Nullable<HashAlgorithm>& legacyOperationLevelOptions)
  {
    if (operationLevelOptions.ChecksumAlgorithm == StorageChecksumAlgorithm::Md5)
    {
      return StorageChecksumAlgorithm::Md5;
    }
    if (operationLevelOptions.ChecksumAlgorithm == StorageChecksumAlgorithm::StorageCrc64)
    {
      return StorageChecksumAlgorithm::StorageCrc64;
    }
    if (clientLevelOptions.Download.ChecksumAlgorithm == StorageChecksumAlgorithm::Md5)
    {
      return StorageChecksumAlgorithm::Md5;
    }
    if (clientLevelOptions.Download.ChecksumAlgorithm == StorageChecksumAlgorithm::StorageCrc64)
    {
      return StorageChecksumAlgorithm::StorageCrc64;
    }
    if (legacyOperationLevelOptions.HasValue()
        && legacyOperationLevelOptions.Value() == HashAlgorithm::Md5)
    {
      return StorageChecksumAlgorithm::Md5;
    }
    if (legacyOperationLevelOptions.HasValue()
        && legacyOperationLevelOptions.Value() == HashAlgorithm::Crc64)
    {
      return StorageChecksumAlgorithm::StorageCrc64;
    }
    return StorageChecksumAlgorithm::None;
  }

  namespace {
    std::pair<StorageChecksumAlgorithm, std::vector<uint8_t>> GetChecksumForUploadOperationImpl(
        const TransferValidationOptions& clientLevelOptions,
        const UploadTransferValidationOptions& operationLevelOptions,
        const Nullable<ContentHash>& legacyOperationLevelOptions)
    {
      if (operationLevelOptions.ChecksumAlgorithm == StorageChecksumAlgorithm::Md5)
      {
        return std::make_pair(
            StorageChecksumAlgorithm::Md5, operationLevelOptions.PrecalculatedChecksum);
      }
      if (operationLevelOptions.ChecksumAlgorithm == StorageChecksumAlgorithm::StorageCrc64)
      {
        return std::make_pair(
            StorageChecksumAlgorithm::StorageCrc64, operationLevelOptions.PrecalculatedChecksum);
      }
      if (clientLevelOptions.Upload.ChecksumAlgorithm == StorageChecksumAlgorithm::Md5)
      {
        return std::make_pair(
            StorageChecksumAlgorithm::Md5,
            legacyOperationLevelOptions.HasValue()
                    && legacyOperationLevelOptions.Value().Algorithm == HashAlgorithm::Md5
                ? legacyOperationLevelOptions.Value().Value
                : std::vector<uint8_t>());
      }
      if (clientLevelOptions.Upload.ChecksumAlgorithm == StorageChecksumAlgorithm::StorageCrc64)
      {
        return std::make_pair(
            StorageChecksumAlgorithm::StorageCrc64,
            legacyOperationLevelOptions.HasValue()
                    && legacyOperationLevelOptions.Value().Algorithm == HashAlgorithm::Crc64
                ? legacyOperationLevelOptions.Value().Value
                : std::vector<uint8_t>());
      }
      if (legacyOperationLevelOptions.HasValue()
          && legacyOperationLevelOptions.Value().Algorithm == HashAlgorithm::Md5)
      {
        return std::make_pair(
            StorageChecksumAlgorithm::Md5, legacyOperationLevelOptions.Value().Value);
      }
      if (legacyOperationLevelOptions.HasValue()
          && legacyOperationLevelOptions.Value().Algorithm == HashAlgorithm::Crc64)
      {
        return std::make_pair(
            StorageChecksumAlgorithm::StorageCrc64, legacyOperationLevelOptions.Value().Value);
      }
      return std::make_pair(StorageChecksumAlgorithm::None, std::vector<uint8_t>());
    }
  } // namespace

  std::pair<StorageChecksumAlgorithm, std::vector<uint8_t>> GetChecksumForUploadOperation(
      const TransferValidationOptions& clientLevelOptions,
      const UploadTransferValidationOptions& operationLevelOptions,
      const Nullable<ContentHash>& legacyOperationLevelOptions,
      Core::IO::BodyStream& content,
      const Core::Context& context)
  {
    auto calculateChecksum = [&](auto& calculator) -> std::vector<uint8_t> {
      std::vector<uint8_t> buffer;
      buffer.resize(1 * 1024 * 1024);
      while (true)
      {
        size_t bytesRead = content.Read(buffer.data(), buffer.size(), context);
        if (bytesRead == 0)
        {
          break;
        }
        calculator.Append(buffer.data(), bytesRead);
      }
      content.Rewind();
      return calculator.Final();
    };

    auto ret = GetChecksumForUploadOperationImpl(
        clientLevelOptions, operationLevelOptions, legacyOperationLevelOptions);

    if (ret.first == StorageChecksumAlgorithm::Md5 && ret.second.empty())
    {
      Core::Cryptography::Md5Hash checksumCalculator;
      ret.second = calculateChecksum(checksumCalculator);
    }
    else if (ret.first == StorageChecksumAlgorithm::StorageCrc64 && ret.second.empty())
    {
      Crc64Hash checksumCalculator;
      ret.second = calculateChecksum(checksumCalculator);
    }

    AZURE_ASSERT(ret.first == StorageChecksumAlgorithm::None || !ret.second.empty());

    return ret;
  }

  std::pair<StorageChecksumAlgorithm, std::vector<uint8_t>> GetChecksumForUploadOperation(
      const TransferValidationOptions& clientLevelOptions,
      const UploadTransferValidationOptions& operationLevelOptions,
      const Nullable<ContentHash>& legacyOperationLevelOptions,
      const uint8_t* content,
      size_t contentSize)
  {
    auto ret = GetChecksumForUploadOperationImpl(
        clientLevelOptions, operationLevelOptions, legacyOperationLevelOptions);

    if (ret.first == StorageChecksumAlgorithm::Md5 && ret.second.empty())
    {
      Core::Cryptography::Md5Hash checksumCalculator;
      checksumCalculator.Append(content, contentSize);
      ret.second = checksumCalculator.Final();
    }
    else if (ret.first == StorageChecksumAlgorithm::StorageCrc64 && ret.second.empty())
    {
      Crc64Hash checksumCalculator;
      checksumCalculator.Append(content, contentSize);
      ret.second = checksumCalculator.Final();
    }

    AZURE_ASSERT(ret.first == StorageChecksumAlgorithm::None || !ret.second.empty());

    return ret;
  }

}}} // namespace Azure::Storage::_internal
