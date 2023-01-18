// Copyright (c) Microsoft Corporation. All rights reserved.
// SPDX-License-Identifier: MIT

#include <cstring>

#include <azure/storage/common/transfer_validation.hpp>

#include "test_base.hpp"

namespace Azure { namespace Storage { namespace Test {

  class TransferValidationCommonTest : public StorageTest {
  };

  TEST_F(TransferValidationCommonTest, GetChecksumAlgorithmForDownloadOperation)
  {
    TransferValidationOptions clientLevelOptions;
    DownloadTransferValidationOptions operationLevelOptions;
    Nullable<HashAlgorithm> legacyOptions;

    auto reset = [&]() {
      clientLevelOptions = TransferValidationOptions();
      operationLevelOptions = DownloadTransferValidationOptions();
      legacyOptions.Reset();
    };

    /*
     * ------------------------------
     * | case | operation | client | legacy |
     * |    1 | None      | None   | Null   |
     * |    2 | None      | Auto   | Null   |
     * |    3 | None      | Set    | Null   |
     * |    4 | Auto      | None   | Null   |
     * |    5 | Auto      | Auto   | Null   |
     * |    6 | Auto      | Set    | Null   |
     * |    7 | Set       | None   | Null   |
     * |    8 | Set       | Auto   | Null   |
     * |    9 | Set       | Set    | Null   |
     * |   10 | None      | None   | Set    |
     * |   11 | None      | Auto   | Set    |
     * |   12 | None      | Set    | Set    |
     * |   13 | Auto      | None   | Set    |
     * |   14 | Auto      | Auto   | Set    |
     * |   15 | Auto      | Set    | Set    |
     * |   16 | Set       | None   | Set    |
     * |   17 | Set       | Auto   | Set    |
     * |   18 | Set       | Set    | Set    |
     */

    // operation-level overrides client-level
    // This covers Cases 1, 2, 3, 7, 8, 9, 10, 11, 12, 16, 17, 18
    for (auto clientLevelAlgorithm :
         {StorageChecksumAlgorithm::Auto,
          StorageChecksumAlgorithm::None,
          StorageChecksumAlgorithm::Md5,
          StorageChecksumAlgorithm::StorageCrc64})
    {
      for (auto operationLevelAlgorithm :
           {StorageChecksumAlgorithm::None,
            StorageChecksumAlgorithm::Md5,
            StorageChecksumAlgorithm::StorageCrc64})
      {
        reset();
        clientLevelOptions.Download.ChecksumAlgorithm = clientLevelAlgorithm;
        operationLevelOptions.ChecksumAlgorithm = operationLevelAlgorithm;
        auto algorithm = _internal::GetChecksumAlgorithmForDownloadOperation(
            clientLevelOptions, operationLevelOptions, legacyOptions);
        EXPECT_EQ(algorithm, operationLevelAlgorithm);

        legacyOptions = HashAlgorithm::Md5;
        algorithm = _internal::GetChecksumAlgorithmForDownloadOperation(
            clientLevelOptions, operationLevelOptions, legacyOptions);
        EXPECT_EQ(algorithm, operationLevelAlgorithm);
      }
    }

    // operation-level is auto, respect client-level
    // This covers Cases 4, 6, 13, 15
    for (auto clientLevelAlgorithm :
         {StorageChecksumAlgorithm::None,
          StorageChecksumAlgorithm::Md5,
          StorageChecksumAlgorithm::StorageCrc64})
    {
      reset();
      clientLevelOptions.Download.ChecksumAlgorithm = clientLevelAlgorithm;
      operationLevelOptions.ChecksumAlgorithm = StorageChecksumAlgorithm::Auto;
      auto algorithm = _internal::GetChecksumAlgorithmForDownloadOperation(
          clientLevelOptions, operationLevelOptions, legacyOptions);
      EXPECT_EQ(algorithm, clientLevelAlgorithm);

      legacyOptions = HashAlgorithm::Crc64;
      algorithm = _internal::GetChecksumAlgorithmForDownloadOperation(
          clientLevelOptions, operationLevelOptions, legacyOptions);
      EXPECT_EQ(algorithm, clientLevelAlgorithm);
    }

    // both operation-level and client-level are auto, with legacy unset
    // This covers Case 5
    {
      reset();
      clientLevelOptions.Download.ChecksumAlgorithm = StorageChecksumAlgorithm::Auto;
      operationLevelOptions.ChecksumAlgorithm = StorageChecksumAlgorithm::Auto;
      auto algorithm = _internal::GetChecksumAlgorithmForDownloadOperation(
          clientLevelOptions, operationLevelOptions, legacyOptions);
      EXPECT_EQ(algorithm, StorageChecksumAlgorithm::None);
    }

    // both operation-level and client-level are auto, with legacy set
    // This covers Case 14
    {
      reset();
      clientLevelOptions.Download.ChecksumAlgorithm = StorageChecksumAlgorithm::Auto;
      operationLevelOptions.ChecksumAlgorithm = StorageChecksumAlgorithm::Auto;
      legacyOptions = HashAlgorithm();
      legacyOptions.Value() = HashAlgorithm::Md5;
      auto algorithm = _internal::GetChecksumAlgorithmForDownloadOperation(
          clientLevelOptions, operationLevelOptions, legacyOptions);
      EXPECT_EQ(algorithm, StorageChecksumAlgorithm::Md5);
    }
    {
      reset();
      clientLevelOptions.Download.ChecksumAlgorithm = StorageChecksumAlgorithm::Auto;
      operationLevelOptions.ChecksumAlgorithm = StorageChecksumAlgorithm::Auto;
      legacyOptions = HashAlgorithm();
      legacyOptions.Value() = HashAlgorithm::Crc64;
      auto algorithm = _internal::GetChecksumAlgorithmForDownloadOperation(
          clientLevelOptions, operationLevelOptions, legacyOptions);
      EXPECT_EQ(algorithm, StorageChecksumAlgorithm::StorageCrc64);
    }
  }

  TEST_F(TransferValidationCommonTest, VectorBodyStream)
  {
    auto buffer = RandomBuffer(1_MB + 1234);
    auto bufferCopy = buffer;
    auto bodyStream = std::make_unique<_internal::VectorBodyStream>(std::move(buffer));
    buffer = bufferCopy;
    EXPECT_EQ(bodyStream->Length(), buffer.size());
    EXPECT_EQ(bodyStream->ReadToEnd(), buffer);
    bodyStream->Rewind();
    EXPECT_EQ(bodyStream->ReadToEnd(), buffer);
  }

  TEST_F(TransferValidationCommonTest, ChecksumBodyStream)
  {
    auto buffer = RandomBuffer(8_MB);
    Core::Cryptography::Md5Hash md5Hasher;
    md5Hasher.Append(buffer.data(), buffer.size());
    auto md5 = md5Hasher.Final();

    Crc64Hash crc64Hasher;
    crc64Hasher.Append(buffer.data(), buffer.size());
    auto crc64 = crc64Hasher.Final();

    {
      ContentHash hash;
      hash.Algorithm = HashAlgorithm::Md5;
      hash.Value = md5;
      auto innerBodyStream = std::make_unique<Core::IO::MemoryBodyStream>(buffer.data(), buffer.size());
      _internal::ChecksumBodyStream checksumBodyStream(std::move(innerBodyStream), hash);
      EXPECT_EQ(checksumBodyStream.Length(), buffer.size());
      auto content = checksumBodyStream.ReadToEnd();
      EXPECT_EQ(content, buffer);
      EXPECT_NO_THROW(checksumBodyStream.Verify());
    }
    {
      ContentHash hash;
      hash.Algorithm = HashAlgorithm::Crc64;
      hash.Value = crc64;
      auto innerBodyStream
          = std::make_unique<Core::IO::MemoryBodyStream>(buffer.data(), buffer.size());
      _internal::ChecksumBodyStream checksumBodyStream(std::move(innerBodyStream), hash);
      EXPECT_EQ(checksumBodyStream.Length(), buffer.size());
      auto content = checksumBodyStream.ReadToEnd();
      EXPECT_EQ(content, buffer);
      EXPECT_NO_THROW(checksumBodyStream.Verify());
    }
    {
      ContentHash hash;
      hash.Algorithm = HashAlgorithm::Md5;
      hash.Value = Core::Convert::Base64Decode(DummyMd5);
      auto innerBodyStream
          = std::make_unique<Core::IO::MemoryBodyStream>(buffer.data(), buffer.size());
      _internal::ChecksumBodyStream checksumBodyStream(std::move(innerBodyStream), hash);
      EXPECT_EQ(checksumBodyStream.Length(), buffer.size());
      auto content = checksumBodyStream.ReadToEnd();
      EXPECT_EQ(content, buffer);
      EXPECT_THROW(checksumBodyStream.Verify(), Core::RequestFailedException);
    }
    {
      ContentHash hash;
      hash.Algorithm = HashAlgorithm::Crc64;
      hash.Value = Core::Convert::Base64Decode(DummyCrc64);
      auto innerBodyStream
          = std::make_unique<Core::IO::MemoryBodyStream>(buffer.data(), buffer.size());
      _internal::ChecksumBodyStream checksumBodyStream(std::move(innerBodyStream), hash);
      EXPECT_EQ(checksumBodyStream.Length(), buffer.size());
      auto content = checksumBodyStream.ReadToEnd();
      EXPECT_EQ(content, buffer);
      EXPECT_THROW(checksumBodyStream.Verify(), Core::RequestFailedException);
    }
    {
      ContentHash hash;
      hash.Algorithm = HashAlgorithm::Md5;
      hash.Value = md5;
      auto innerBodyStream
          = std::make_unique<Core::IO::MemoryBodyStream>(buffer.data(), buffer.size());
      _internal::ChecksumBodyStream checksumBodyStream(std::move(innerBodyStream), hash);
      std::vector<uint8_t> tempBuffer;
      tempBuffer.resize(buffer.size());
      checksumBodyStream.ReadToCount(tempBuffer.data(), tempBuffer.size(), Core::Context());
      EXPECT_NO_THROW(checksumBodyStream.Verify());
    }
  }

  TEST_F(TransferValidationCommonTest, GetChecksumForUploadOperation)
  {
    std::vector<uint8_t> buffer = RandomBuffer(1_KB);
    Core::IO::MemoryBodyStream bufferBodyStream(buffer.data(), buffer.size());
    Core::Cryptography::Md5Hash md5Hasher;
    md5Hasher.Append(buffer.data(), buffer.size());
    auto md5 = md5Hasher.Final();

    Crc64Hash crc64Hasher;
    crc64Hasher.Append(buffer.data(), buffer.size());
    auto crc64 = crc64Hasher.Final();

    TransferValidationOptions clientLevelOptions;
    UploadTransferValidationOptions operationLevelOptions;
    Nullable<ContentHash> legacyOptions;

    auto reset = [&]() {
      clientLevelOptions = TransferValidationOptions();
      operationLevelOptions = UploadTransferValidationOptions();
      legacyOptions.Reset();
    };

    auto verify = [&](const TransferValidationOptions& clientLevelOptions,
                      const UploadTransferValidationOptions& operationLevelOptions,
                      const Nullable<ContentHash>& legacyOperationLevelOptions) {
      auto ret1 = _internal::GetChecksumForUploadOperation(
          clientLevelOptions,
          operationLevelOptions,
          legacyOperationLevelOptions,
          bufferBodyStream,
          Core::Context());
      auto ret2 = _internal::GetChecksumForUploadOperation(
          clientLevelOptions,
          operationLevelOptions,
          legacyOperationLevelOptions,
          buffer.data(),
          buffer.size());
      EXPECT_EQ(ret1, ret2);
      if (ret1.first == StorageChecksumAlgorithm::Md5)
      {
        EXPECT_FALSE(ret1.second.empty());
      }
      else if (ret1.first == StorageChecksumAlgorithm::StorageCrc64)
      {
        EXPECT_FALSE(ret1.second.empty());
      }
      else if (ret1.first == StorageChecksumAlgorithm::None)
      {
        EXPECT_TRUE(ret1.second.empty());
      }
      return ret1;
    };

    /*
     * ------------------------------
     * | case | operation | client | legacy |
     * |    1 | None      | None   | Null   |
     * |    2 | None      | Auto   | Null   |
     * |    3 | None      | Set    | Null   |
     * |    4 | Auto      | None   | Null   |
     * |    5 | Auto      | Auto   | Null   |
     * |    6 | Auto      | Set    | Null   |
     * |    7 | Set       | None   | Null   |
     * |    8 | Set       | Auto   | Null   |
     * |    9 | Set       | Set    | Null   |
     * |   10 | None      | None   | Set    |
     * |   11 | None      | Auto   | Set    |
     * |   12 | None      | Set    | Set    |
     * |   13 | Auto      | None   | Set    |
     * |   14 | Auto      | Auto   | Set    |
     * |   15 | Auto      | Set    | Set    |
     * |   16 | Set       | None   | Set    |
     * |   17 | Set       | Auto   | Set    |
     * |   18 | Set       | Set    | Set    |
     */

    // operation-level overrides client-level
    // This covers Cases 1, 2, 3, 7, 8, 9, 10, 11, 12, 16, 17, 18
    for (auto clientLevelAlgorithm :
         {StorageChecksumAlgorithm::Auto,
          StorageChecksumAlgorithm::None,
          StorageChecksumAlgorithm::Md5,
          StorageChecksumAlgorithm::StorageCrc64})
    {
      for (auto operationLevelAlgorithm :
           {StorageChecksumAlgorithm::None,
            StorageChecksumAlgorithm::Md5,
            StorageChecksumAlgorithm::StorageCrc64})
      {
        reset();
        clientLevelOptions.Upload.ChecksumAlgorithm = clientLevelAlgorithm;
        operationLevelOptions.ChecksumAlgorithm = operationLevelAlgorithm;

        auto ret = verify(clientLevelOptions, operationLevelOptions, legacyOptions);
        EXPECT_EQ(ret.first, operationLevelAlgorithm);
        if (ret.first == StorageChecksumAlgorithm::Md5)
        {
          EXPECT_EQ(ret.second, md5);
        }
        else if (ret.first == StorageChecksumAlgorithm::StorageCrc64)
        {
          EXPECT_EQ(ret.second, crc64);
        }
        else if (ret.first == StorageChecksumAlgorithm::None)
        {
          EXPECT_TRUE(ret.second.empty());
        }

        reset();
        clientLevelOptions.Upload.ChecksumAlgorithm = clientLevelAlgorithm;
        operationLevelOptions.ChecksumAlgorithm = operationLevelAlgorithm;
        if (operationLevelAlgorithm == StorageChecksumAlgorithm::Md5)
        {
          operationLevelOptions.PrecalculatedChecksum = Core::Convert::Base64Decode(DummyMd5);
        }
        else if (operationLevelAlgorithm == StorageChecksumAlgorithm::StorageCrc64)
        {
          operationLevelOptions.PrecalculatedChecksum = Core::Convert::Base64Decode(DummyCrc64);
        }
        ret = verify(clientLevelOptions, operationLevelOptions, legacyOptions);
        EXPECT_EQ(ret.first, operationLevelAlgorithm);
        if (ret.first == StorageChecksumAlgorithm::Md5)
        {
          EXPECT_EQ(ret.second, Core::Convert::Base64Decode(DummyMd5));
        }
        else if (ret.first == StorageChecksumAlgorithm::StorageCrc64)
        {
          EXPECT_EQ(ret.second, Core::Convert::Base64Decode(DummyCrc64));
        }
        else if (ret.first == StorageChecksumAlgorithm::None)
        {
          EXPECT_TRUE(ret.second.empty());
        }

        reset();
        clientLevelOptions.Upload.ChecksumAlgorithm = clientLevelAlgorithm;
        operationLevelOptions.ChecksumAlgorithm = operationLevelAlgorithm;
        if (operationLevelAlgorithm == StorageChecksumAlgorithm::Md5)
        {
          legacyOptions = ContentHash();
          legacyOptions.Value().Algorithm = HashAlgorithm::Md5;
          legacyOptions.Value().Value = Core::Convert::Base64Decode(DummyMd5);
        }
        else if (operationLevelAlgorithm == StorageChecksumAlgorithm::StorageCrc64)
        {
          legacyOptions = ContentHash();
          legacyOptions.Value().Algorithm = HashAlgorithm::Crc64;
          legacyOptions.Value().Value = Core::Convert::Base64Decode(DummyCrc64);
        }
        ret = verify(clientLevelOptions, operationLevelOptions, legacyOptions);
        EXPECT_EQ(ret.first, operationLevelAlgorithm);
        if (ret.first == StorageChecksumAlgorithm::Md5)
        {
          EXPECT_EQ(ret.second, md5);
        }
        else if (ret.first == StorageChecksumAlgorithm::StorageCrc64)
        {
          EXPECT_EQ(ret.second, crc64);
        }
        else if (ret.first == StorageChecksumAlgorithm::None)
        {
          EXPECT_TRUE(ret.second.empty());
        }
      }
    }

    // operation-level is auto, respect client-level
    // This covers Cases 4, 6, 13, 15
    for (auto clientLevelAlgorithm :
         {StorageChecksumAlgorithm::None,
          StorageChecksumAlgorithm::Md5,
          StorageChecksumAlgorithm::StorageCrc64})
    {
      reset();
      clientLevelOptions.Upload.ChecksumAlgorithm = clientLevelAlgorithm;
      operationLevelOptions.ChecksumAlgorithm = StorageChecksumAlgorithm::Auto;
      auto ret = verify(clientLevelOptions, operationLevelOptions, legacyOptions);
      EXPECT_EQ(ret.first, clientLevelAlgorithm);
      if (ret.first == StorageChecksumAlgorithm::Md5)
      {
        EXPECT_EQ(ret.second, md5);
      }
      else if (ret.first == StorageChecksumAlgorithm::StorageCrc64)
      {
        EXPECT_EQ(ret.second, crc64);
      }
      else if (ret.first == StorageChecksumAlgorithm::None)
      {
        EXPECT_TRUE(ret.second.empty());
      }

      if (clientLevelAlgorithm == StorageChecksumAlgorithm::Md5)
      {
        legacyOptions = ContentHash();
        legacyOptions.Value().Algorithm = HashAlgorithm::Md5;
        legacyOptions.Value().Value = Core::Convert::Base64Decode(DummyMd5);
      }
      else if (clientLevelAlgorithm == StorageChecksumAlgorithm::StorageCrc64)
      {
        legacyOptions = ContentHash();
        legacyOptions.Value().Algorithm = HashAlgorithm::Crc64;
        legacyOptions.Value().Value = Core::Convert::Base64Decode(DummyCrc64);
      }
      ret = verify(clientLevelOptions, operationLevelOptions, legacyOptions);
      EXPECT_EQ(ret.first, clientLevelAlgorithm);
      if (ret.first == StorageChecksumAlgorithm::Md5)
      {
        EXPECT_EQ(ret.second, Core::Convert::Base64Decode(DummyMd5));
      }
      else if (ret.first == StorageChecksumAlgorithm::StorageCrc64)
      {
        EXPECT_EQ(ret.second, Core::Convert::Base64Decode(DummyCrc64));
      }
      else if (ret.first == StorageChecksumAlgorithm::None)
      {
        EXPECT_TRUE(ret.second.empty());
      }
    }

    // both operation-level and client-level are auto, with legacy unset
    // This covers Case 5
    {
      reset();
      clientLevelOptions.Upload.ChecksumAlgorithm = StorageChecksumAlgorithm::Auto;
      operationLevelOptions.ChecksumAlgorithm = StorageChecksumAlgorithm::Auto;
      auto ret = verify(clientLevelOptions, operationLevelOptions, legacyOptions);
      EXPECT_EQ(ret.first, StorageChecksumAlgorithm::None);
      EXPECT_TRUE(ret.second.empty());
    }

    // both operation-level and client-level are auto, with legacy set
    // This covers Case 14
    {
      reset();
      clientLevelOptions.Upload.ChecksumAlgorithm = StorageChecksumAlgorithm::Auto;
      operationLevelOptions.ChecksumAlgorithm = StorageChecksumAlgorithm::Auto;
      legacyOptions = ContentHash();
      legacyOptions.Value().Algorithm = HashAlgorithm::Crc64;
      legacyOptions.Value().Value = Core::Convert::Base64Decode(DummyCrc64);
      auto ret = verify(clientLevelOptions, operationLevelOptions, legacyOptions);
      EXPECT_EQ(ret.first, StorageChecksumAlgorithm::StorageCrc64);
      EXPECT_EQ(ret.second, Core::Convert::Base64Decode(DummyCrc64));
    }
    {
      reset();
      clientLevelOptions.Upload.ChecksumAlgorithm = StorageChecksumAlgorithm::Auto;
      operationLevelOptions.ChecksumAlgorithm = StorageChecksumAlgorithm::Auto;
      legacyOptions = ContentHash();
      legacyOptions.Value().Algorithm = HashAlgorithm::Md5;
      legacyOptions.Value().Value = Core::Convert::Base64Decode(DummyMd5);
      auto ret = verify(clientLevelOptions, operationLevelOptions, legacyOptions);
      EXPECT_EQ(ret.first, StorageChecksumAlgorithm::Md5);
      EXPECT_EQ(ret.second, Core::Convert::Base64Decode(DummyMd5));
    }
  }

}}} // namespace Azure::Storage::Test
