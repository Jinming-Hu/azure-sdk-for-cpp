// Copyright (c) Microsoft Corporation. All rights reserved.
// SPDX-License-Identifier: MIT

#include "append_blob_client_test.hpp"
#include "block_blob_client_test.hpp"
#include "page_blob_client_test.hpp"

namespace Azure { namespace Storage { namespace Test {

  class ModifyTransactioalHashHeaderPolicy final : public Core::Http::Policies::HttpPolicy {
  public:
    ModifyTransactioalHashHeaderPolicy() {}
    ~ModifyTransactioalHashHeaderPolicy() override {}

    std::unique_ptr<HttpPolicy> Clone() const override
    {
      return std::make_unique<ModifyTransactioalHashHeaderPolicy>(*this);
    }

    std::unique_ptr<Core::Http::RawResponse> Send(
        Core::Http::Request& request,
        Core::Http::Policies::NextHttpPolicy nextPolicy,
        Core::Context const& context) const override
    {
      auto requestHeaders = request.GetHeaders();
      for (const auto& header : {"Content-MD5", "x-ms-content-crc64"})
      {
        auto ite = requestHeaders.find(header);
        if (ite != requestHeaders.end())
        {
          auto value = ite->second;
          value[0] = value[0] == 'A' ? 'B' : 'A';
          request.SetHeader(header, value);
        }
      }
      auto response = nextPolicy.Send(request, context);
      const auto& responseHeaders = response->GetHeaders();
      for (const auto& header : {"Content-MD5", "x-ms-content-crc64"})
      {

        auto ite = responseHeaders.find(header);
        if (ite != responseHeaders.end())
        {
          auto value = ite->second;
          value[0] = value[0] == 'A' ? 'B' : 'A';
          response->SetHeader(header, value);
        }
      }
      return response;
    }
  };

  TEST_F(BlockBlobClientTest, DownloadTransferValidation)
  {
    auto blobClient = GetBlockBlobClient("aaaa");
    auto buffer = RandomBuffer(100);
    blobClient.UploadFrom(buffer.data(), buffer.size());

    Blobs::DownloadBlobOptions downloadOptions;
    downloadOptions.Range = Core::Http::HttpRange();
    downloadOptions.Range.Value().Offset = 0;
    downloadOptions.Range.Value().Length = buffer.size();
    downloadOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::Md5;
    auto downloadResponse = blobClient.Download(downloadOptions);
    EXPECT_TRUE(downloadResponse.Value.TransactionalContentHash.HasValue());
    EXPECT_EQ(
        downloadResponse.Value.TransactionalContentHash.Value().Algorithm, HashAlgorithm::Md5);
    EXPECT_FALSE(downloadResponse.Value.TransactionalContentHash.Value().Value.empty());

    downloadOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::StorageCrc64;
    downloadResponse = blobClient.Download(downloadOptions);
    EXPECT_TRUE(downloadResponse.Value.TransactionalContentHash.HasValue());
    EXPECT_EQ(
        downloadResponse.Value.TransactionalContentHash.Value().Algorithm, HashAlgorithm::Crc64);
    EXPECT_FALSE(downloadResponse.Value.TransactionalContentHash.Value().Value.empty());

    auto clientOptions = Blobs::BlobClientOptions();
    clientOptions.PerOperationPolicies.push_back(
        std::make_unique<ModifyTransactioalHashHeaderPolicy>());
    auto blobClientWithModifiedContentHash
        = Blobs::BlockBlobClient(blobClient.GetUrl() + GetSas(), clientOptions);
    EXPECT_THROW(blobClientWithModifiedContentHash.Download(downloadOptions), std::runtime_error);
    downloadOptions.TransferValidation.AutoValidateChecksum = false;
    EXPECT_NO_THROW(blobClientWithModifiedContentHash.Download(downloadOptions));
    downloadOptions.TransferValidation.AutoValidateChecksum = true;
    EXPECT_THROW(blobClientWithModifiedContentHash.Download(downloadOptions), std::runtime_error);

    downloadOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::None;
    downloadOptions.TransferValidation.AutoValidateChecksum = true;
    EXPECT_NO_THROW(blobClientWithModifiedContentHash.Download(downloadOptions));
  }

  // TEST_F(BlockBlobClientTest, DownloadToBufferTransferValidation) {
  //   auto blobClient = GetBlockBlobClient("aaaa");
  //   auto buffer = RandomBuffer(100);
  //   blobClient.UploadFrom(buffer.data(), buffer.size());
  //
  //   Blobs::DownloadBlobToOptions downloadOptions;
  //   downloadOptions.TransferOptions.InitialChunkSize = 10;
  //   downloadOptions.TransferOptions.ChunkSize = 15;
  //   for (auto algo :
  //        {StorageChecksumAlgorithm::Auto,
  //         StorageChecksumAlgorithm::None,
  //         StorageChecksumAlgorithm::Md5,
  //         StorageChecksumAlgorithm::StorageCrc64})
  //   {
  //     downloadOptions.TransferValidation.ChecksumAlgorithm = algo;
  //     downloadOptions.TransferValidation.AutoValidateChecksum = false;
  //     std::vector<uint8_t> tempBuffer;
  //     tempBuffer.resize(buffer.size());
  //     blobClient.DownloadTo(tempBuffer.data(), tempBuffer.size(), downloadOptions);
  //   }
  // }

  TEST_F(BlockBlobClientTest, UploadTransferValidation)
  {
    auto blobClient = GetBlockBlobClient("aaaa");
    auto buffer = RandomBuffer(100);
    Core::Cryptography::Md5Hash md5Hasher;
    md5Hasher.Append(buffer.data(), buffer.size());
    auto md5 = md5Hasher.Final();

    Crc64Hash crc64Hasher;
    crc64Hasher.Append(buffer.data(), buffer.size());
    auto crc64 = crc64Hasher.Final();
    Core::IO::MemoryBodyStream bodyStream(buffer.data(), buffer.size());

    auto clientOptions = Blobs::BlobClientOptions();
    clientOptions.PerOperationPolicies.push_back(
        std::make_unique<ModifyTransactioalHashHeaderPolicy>());
    auto blobClientWithModifiedContentHash
        = Blobs::BlockBlobClient(blobClient.GetUrl() + GetSas(), clientOptions);

    Blobs::UploadBlockBlobOptions uploadOptions;

    auto reset = [&]() {
      bodyStream.Rewind();
      uploadOptions = Blobs::UploadBlockBlobOptions();
    };

    reset();
    uploadOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::Auto;
    EXPECT_NO_THROW(blobClient.Upload(bodyStream, uploadOptions));

    reset();
    uploadOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::Auto;
    uploadOptions.TransactionalContentHash = ContentHash();
    uploadOptions.TransactionalContentHash.Value().Algorithm = HashAlgorithm::Md5;
    uploadOptions.TransactionalContentHash.Value().Value = Core::Convert::Base64Decode(DummyMd5);
    EXPECT_THROW(blobClient.Upload(bodyStream, uploadOptions), StorageException);

    reset();
    uploadOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::Auto;
    uploadOptions.TransactionalContentHash = ContentHash();
    uploadOptions.TransactionalContentHash.Value().Algorithm = HashAlgorithm::Crc64;
    uploadOptions.TransactionalContentHash.Value().Value = Core::Convert::Base64Decode(DummyCrc64);
    EXPECT_THROW(blobClient.Upload(bodyStream, uploadOptions), StorageException);

    reset();
    uploadOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::None;
    EXPECT_NO_THROW(blobClient.Upload(bodyStream, uploadOptions));

    reset();
    uploadOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::None;
    uploadOptions.TransferValidation.PrecalculatedChecksum = Core::Convert::Base64Decode(DummyMd5);
    EXPECT_NO_THROW(blobClient.Upload(bodyStream, uploadOptions));

    reset();
    uploadOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::None;
    uploadOptions.TransactionalContentHash = ContentHash();
    uploadOptions.TransactionalContentHash.Value().Algorithm = HashAlgorithm::Md5;
    uploadOptions.TransactionalContentHash.Value().Value = Core::Convert::Base64Decode(DummyMd5);
    EXPECT_NO_THROW(blobClient.Upload(bodyStream, uploadOptions));

    reset();
    uploadOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::None;
    uploadOptions.TransactionalContentHash = ContentHash();
    uploadOptions.TransactionalContentHash.Value().Algorithm = HashAlgorithm::Crc64;
    uploadOptions.TransactionalContentHash.Value().Value = Core::Convert::Base64Decode(DummyCrc64);
    EXPECT_NO_THROW(blobClient.Upload(bodyStream, uploadOptions));

    reset();
    uploadOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::Md5;
    EXPECT_NO_THROW(blobClient.Upload(bodyStream, uploadOptions));
    EXPECT_THROW(
        blobClientWithModifiedContentHash.Upload(bodyStream, uploadOptions), StorageException);

    reset();
    uploadOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::Md5;
    uploadOptions.TransferValidation.PrecalculatedChecksum = md5;
    EXPECT_NO_THROW(blobClient.Upload(bodyStream, uploadOptions));
    EXPECT_THROW(
        blobClientWithModifiedContentHash.Upload(bodyStream, uploadOptions), StorageException);

    reset();
    uploadOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::Md5;
    uploadOptions.TransferValidation.PrecalculatedChecksum = Core::Convert::Base64Decode(DummyMd5);
    EXPECT_THROW(blobClient.Upload(bodyStream, uploadOptions), StorageException);

    reset();
    uploadOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::StorageCrc64;
    EXPECT_NO_THROW(blobClient.Upload(bodyStream, uploadOptions));
    EXPECT_THROW(
        blobClientWithModifiedContentHash.Upload(bodyStream, uploadOptions), StorageException);

    reset();
    uploadOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::StorageCrc64;
    uploadOptions.TransferValidation.PrecalculatedChecksum = crc64;
    EXPECT_NO_THROW(blobClient.Upload(bodyStream, uploadOptions));
    EXPECT_THROW(
        blobClientWithModifiedContentHash.Upload(bodyStream, uploadOptions), StorageException);

    reset();
    uploadOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::StorageCrc64;
    uploadOptions.TransferValidation.PrecalculatedChecksum
        = Core::Convert::Base64Decode(DummyCrc64);
    EXPECT_THROW(blobClient.Upload(bodyStream, uploadOptions), StorageException);
  }

  TEST_F(BlockBlobClientTest, UploadFromTransferValidation)
  {
    auto blobClient = GetBlockBlobClient("aaaa");
    auto buffer = RandomBuffer(100);
    const auto fileName = RandomString();
    WriteFile(fileName, buffer);
    Core::Cryptography::Md5Hash md5Hasher;
    md5Hasher.Append(buffer.data(), buffer.size());
    auto md5 = md5Hasher.Final();

    Crc64Hash crc64Hasher;
    crc64Hasher.Append(buffer.data(), buffer.size());
    auto crc64 = crc64Hasher.Final();

    auto clientOptions = Blobs::BlobClientOptions();
    clientOptions.PerOperationPolicies.push_back(
        std::make_unique<ModifyTransactioalHashHeaderPolicy>());
    auto blobClientWithModifiedContentHash
        = Blobs::BlockBlobClient(blobClient.GetUrl() + GetSas(), clientOptions);

    for (int singleUploadThreshold : {-1, 10})
    {
      for (auto chunkSize : {-1, 30})
      {
        Blobs::UploadBlockBlobFromOptions uploadOptions;
        auto reset = [&]() {
          uploadOptions = Blobs::UploadBlockBlobFromOptions();
          if (singleUploadThreshold != -1)
          {
            uploadOptions.TransferOptions.SingleUploadThreshold = singleUploadThreshold;
          }
          if (chunkSize != -1)
          {
            uploadOptions.TransferOptions.ChunkSize = chunkSize;
          }
        };
        reset();
        uploadOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::Auto;
        EXPECT_NO_THROW(blobClient.UploadFrom(buffer.data(), buffer.size(), uploadOptions));
        EXPECT_NO_THROW(blobClient.UploadFrom(fileName, uploadOptions));

        reset();
        uploadOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::None;
        EXPECT_NO_THROW(blobClient.UploadFrom(buffer.data(), buffer.size(), uploadOptions));
        EXPECT_NO_THROW(blobClient.UploadFrom(fileName, uploadOptions));

        reset();
        uploadOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::Md5;
        EXPECT_NO_THROW(blobClient.UploadFrom(buffer.data(), buffer.size(), uploadOptions));
        EXPECT_NO_THROW(blobClient.UploadFrom(fileName, uploadOptions));
        EXPECT_THROW(
            blobClientWithModifiedContentHash.UploadFrom(
                buffer.data(), buffer.size(), uploadOptions),
            StorageException);
        EXPECT_THROW(
            blobClientWithModifiedContentHash.UploadFrom(fileName, uploadOptions),
            StorageException);

        reset();
        uploadOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::Md5;
        uploadOptions.TransferValidation.PrecalculatedChecksum = md5;
        if (singleUploadThreshold >= buffer.size())
        {
          EXPECT_NO_THROW(blobClient.UploadFrom(buffer.data(), buffer.size(), uploadOptions));
          EXPECT_NO_THROW(blobClient.UploadFrom(fileName, uploadOptions));
          EXPECT_THROW(
              blobClientWithModifiedContentHash.UploadFrom(
                  buffer.data(), buffer.size(), uploadOptions),
              StorageException);
          EXPECT_THROW(
              blobClientWithModifiedContentHash.UploadFrom(fileName, uploadOptions),
              StorageException);
        }
        else
        {
          EXPECT_THROW(
              blobClient.UploadFrom(buffer.data(), buffer.size(), uploadOptions),
              Core::RequestFailedException);
          EXPECT_THROW(
              blobClient.UploadFrom(fileName, uploadOptions), Core::RequestFailedException);
        }

        reset();
        uploadOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::Md5;
        uploadOptions.TransferValidation.PrecalculatedChecksum
            = Core::Convert::Base64Decode(DummyMd5);
        if (singleUploadThreshold >= buffer.size())
        {
          EXPECT_THROW(
              blobClient.UploadFrom(buffer.data(), buffer.size(), uploadOptions), StorageException);
          EXPECT_THROW(blobClient.UploadFrom(fileName, uploadOptions), StorageException);
        }
        else
        {
          EXPECT_THROW(
              blobClient.UploadFrom(buffer.data(), buffer.size(), uploadOptions),
              Core::RequestFailedException);
          EXPECT_THROW(
              blobClient.UploadFrom(fileName, uploadOptions), Core::RequestFailedException);
        }

        reset();
        uploadOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::StorageCrc64;
        EXPECT_NO_THROW(blobClient.UploadFrom(buffer.data(), buffer.size(), uploadOptions));
        EXPECT_NO_THROW(blobClient.UploadFrom(fileName, uploadOptions));
        EXPECT_THROW(
            blobClientWithModifiedContentHash.UploadFrom(
                buffer.data(), buffer.size(), uploadOptions),
            StorageException);
        EXPECT_THROW(
            blobClientWithModifiedContentHash.UploadFrom(fileName, uploadOptions),
            StorageException);

        reset();
        uploadOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::StorageCrc64;
        uploadOptions.TransferValidation.PrecalculatedChecksum = crc64;
        if (singleUploadThreshold >= buffer.size())
        {
          EXPECT_NO_THROW(blobClient.UploadFrom(buffer.data(), buffer.size(), uploadOptions));
          EXPECT_NO_THROW(blobClient.UploadFrom(fileName, uploadOptions));
          EXPECT_THROW(
              blobClientWithModifiedContentHash.UploadFrom(
                  buffer.data(), buffer.size(), uploadOptions),
              StorageException);
          EXPECT_THROW(
              blobClientWithModifiedContentHash.UploadFrom(fileName, uploadOptions),
              StorageException);
        }
        else
        {
          EXPECT_THROW(
              blobClient.UploadFrom(buffer.data(), buffer.size(), uploadOptions),
              Core::RequestFailedException);
          EXPECT_THROW(
              blobClient.UploadFrom(fileName, uploadOptions), Core::RequestFailedException);
        }

        reset();
        uploadOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::StorageCrc64;
        uploadOptions.TransferValidation.PrecalculatedChecksum
            = Core::Convert::Base64Decode(DummyCrc64);
        if (singleUploadThreshold >= buffer.size())
        {
          EXPECT_THROW(
              blobClient.UploadFrom(buffer.data(), buffer.size(), uploadOptions), StorageException);
          EXPECT_THROW(blobClient.UploadFrom(fileName, uploadOptions), StorageException);
        }
        else
        {
          EXPECT_THROW(
              blobClient.UploadFrom(buffer.data(), buffer.size(), uploadOptions),
              Core::RequestFailedException);
          EXPECT_THROW(
              blobClient.UploadFrom(fileName, uploadOptions), Core::RequestFailedException);
        }
      }
    }
  }

  TEST_F(BlockBlobClientTest, StageBlockTransferValidation)
  {
    auto blobClient = GetBlockBlobClient("aaaa");
    auto buffer = RandomBuffer(10);
    Core::Cryptography::Md5Hash md5Hasher;
    md5Hasher.Append(buffer.data(), buffer.size());
    auto md5 = md5Hasher.Final();

    Crc64Hash crc64Hasher;
    crc64Hasher.Append(buffer.data(), buffer.size());
    auto crc64 = crc64Hasher.Final();
    Core::IO::MemoryBodyStream bodyStream(buffer.data(), buffer.size());

    auto clientOptions = Blobs::BlobClientOptions();
    clientOptions.PerOperationPolicies.push_back(
        std::make_unique<ModifyTransactioalHashHeaderPolicy>());
    auto blobClientWithModifiedContentHash
        = Blobs::BlockBlobClient(blobClient.GetUrl() + GetSas(), clientOptions);

    const auto blockId = Core::Convert::Base64Encode(RandomBuffer(10));
    Blobs::StageBlockOptions stageBlockOptions;

    auto reset = [&]() {
      bodyStream.Rewind();
      stageBlockOptions = Blobs::StageBlockOptions();
    };

    reset();
    stageBlockOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::Auto;
    EXPECT_NO_THROW(blobClient.StageBlock(blockId, bodyStream, stageBlockOptions));

    reset();
    stageBlockOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::Auto;
    stageBlockOptions.TransactionalContentHash = ContentHash();
    stageBlockOptions.TransactionalContentHash.Value().Algorithm = HashAlgorithm::Md5;
    stageBlockOptions.TransactionalContentHash.Value().Value
        = Core::Convert::Base64Decode(DummyMd5);
    EXPECT_THROW(blobClient.StageBlock(blockId, bodyStream, stageBlockOptions), StorageException);

    reset();
    stageBlockOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::Auto;
    stageBlockOptions.TransactionalContentHash = ContentHash();
    stageBlockOptions.TransactionalContentHash.Value().Algorithm = HashAlgorithm::Crc64;
    stageBlockOptions.TransactionalContentHash.Value().Value
        = Core::Convert::Base64Decode(DummyCrc64);
    EXPECT_THROW(blobClient.StageBlock(blockId, bodyStream, stageBlockOptions), StorageException);

    reset();
    stageBlockOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::None;
    EXPECT_NO_THROW(blobClient.StageBlock(blockId, bodyStream, stageBlockOptions));

    reset();
    stageBlockOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::None;
    stageBlockOptions.TransferValidation.PrecalculatedChecksum
        = Core::Convert::Base64Decode(DummyMd5);
    EXPECT_NO_THROW(blobClient.StageBlock(blockId, bodyStream, stageBlockOptions));

    reset();
    stageBlockOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::None;
    stageBlockOptions.TransactionalContentHash = ContentHash();
    stageBlockOptions.TransactionalContentHash.Value().Algorithm = HashAlgorithm::Md5;
    stageBlockOptions.TransactionalContentHash.Value().Value
        = Core::Convert::Base64Decode(DummyMd5);
    EXPECT_NO_THROW(blobClient.StageBlock(blockId, bodyStream, stageBlockOptions));

    reset();
    stageBlockOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::None;
    stageBlockOptions.TransactionalContentHash = ContentHash();
    stageBlockOptions.TransactionalContentHash.Value().Algorithm = HashAlgorithm::Crc64;
    stageBlockOptions.TransactionalContentHash.Value().Value
        = Core::Convert::Base64Decode(DummyCrc64);
    EXPECT_NO_THROW(blobClient.StageBlock(blockId, bodyStream, stageBlockOptions));

    reset();
    stageBlockOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::Md5;
    EXPECT_NO_THROW(blobClient.StageBlock(
        Core::Convert::Base64Encode(RandomBuffer(10)), bodyStream, stageBlockOptions));
    EXPECT_THROW(
        blobClientWithModifiedContentHash.StageBlock(blockId, bodyStream, stageBlockOptions),
        StorageException);

    reset();
    stageBlockOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::Md5;
    stageBlockOptions.TransferValidation.PrecalculatedChecksum = md5;
    EXPECT_NO_THROW(blobClient.StageBlock(
        Core::Convert::Base64Encode(RandomBuffer(10)), bodyStream, stageBlockOptions));
    EXPECT_THROW(
        blobClientWithModifiedContentHash.StageBlock(blockId, bodyStream, stageBlockOptions),
        StorageException);

    reset();
    stageBlockOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::Md5;
    stageBlockOptions.TransferValidation.PrecalculatedChecksum
        = Core::Convert::Base64Decode(DummyMd5);
    EXPECT_THROW(blobClient.StageBlock(blockId, bodyStream, stageBlockOptions), StorageException);

    reset();
    stageBlockOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::StorageCrc64;
    EXPECT_NO_THROW(blobClient.StageBlock(blockId, bodyStream, stageBlockOptions));
    EXPECT_THROW(
        blobClientWithModifiedContentHash.StageBlock(blockId, bodyStream, stageBlockOptions),
        StorageException);

    reset();
    stageBlockOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::StorageCrc64;
    stageBlockOptions.TransferValidation.PrecalculatedChecksum = crc64;
    EXPECT_NO_THROW(blobClient.StageBlock(blockId, bodyStream, stageBlockOptions));
    EXPECT_THROW(
        blobClientWithModifiedContentHash.StageBlock(blockId, bodyStream, stageBlockOptions),
        StorageException);

    reset();
    stageBlockOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::StorageCrc64;
    stageBlockOptions.TransferValidation.PrecalculatedChecksum
        = Core::Convert::Base64Decode(DummyCrc64);
    EXPECT_THROW(blobClient.StageBlock(blockId, bodyStream, stageBlockOptions), StorageException);
  }

  TEST_F(AppendBlobClientTest, AppendBlockTransferValidation)
  {
    auto blobClient = GetAppendBlobClient("aaaa");
    blobClient.Create();
    auto buffer = RandomBuffer(10);
    Core::Cryptography::Md5Hash md5Hasher;
    md5Hasher.Append(buffer.data(), buffer.size());
    auto md5 = md5Hasher.Final();

    Crc64Hash crc64Hasher;
    crc64Hasher.Append(buffer.data(), buffer.size());
    auto crc64 = crc64Hasher.Final();
    Core::IO::MemoryBodyStream bodyStream(buffer.data(), buffer.size());

    auto clientOptions = Blobs::BlobClientOptions();
    clientOptions.PerOperationPolicies.push_back(
        std::make_unique<ModifyTransactioalHashHeaderPolicy>());
    auto blobClientWithModifiedContentHash
        = Blobs::AppendBlobClient(blobClient.GetUrl() + GetSas(), clientOptions);

    Blobs::AppendBlockOptions uploadOptions;

    auto reset = [&]() {
      bodyStream.Rewind();
      uploadOptions = Blobs::AppendBlockOptions();
    };

    reset();
    uploadOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::Auto;
    EXPECT_NO_THROW(blobClient.AppendBlock(bodyStream, uploadOptions));

    reset();
    uploadOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::Auto;
    uploadOptions.TransactionalContentHash = ContentHash();
    uploadOptions.TransactionalContentHash.Value().Algorithm = HashAlgorithm::Md5;
    uploadOptions.TransactionalContentHash.Value().Value = Core::Convert::Base64Decode(DummyMd5);
    EXPECT_THROW(blobClient.AppendBlock(bodyStream, uploadOptions), StorageException);

    reset();
    uploadOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::Auto;
    uploadOptions.TransactionalContentHash = ContentHash();
    uploadOptions.TransactionalContentHash.Value().Algorithm = HashAlgorithm::Crc64;
    uploadOptions.TransactionalContentHash.Value().Value = Core::Convert::Base64Decode(DummyCrc64);
    EXPECT_THROW(blobClient.AppendBlock(bodyStream, uploadOptions), StorageException);

    reset();
    uploadOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::None;
    EXPECT_NO_THROW(blobClient.AppendBlock(bodyStream, uploadOptions));

    reset();
    uploadOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::None;
    uploadOptions.TransferValidation.PrecalculatedChecksum = Core::Convert::Base64Decode(DummyMd5);
    EXPECT_NO_THROW(blobClient.AppendBlock(bodyStream, uploadOptions));

    reset();
    uploadOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::None;
    uploadOptions.TransactionalContentHash = ContentHash();
    uploadOptions.TransactionalContentHash.Value().Algorithm = HashAlgorithm::Md5;
    uploadOptions.TransactionalContentHash.Value().Value = Core::Convert::Base64Decode(DummyMd5);
    EXPECT_NO_THROW(blobClient.AppendBlock(bodyStream, uploadOptions));

    reset();
    uploadOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::None;
    uploadOptions.TransactionalContentHash = ContentHash();
    uploadOptions.TransactionalContentHash.Value().Algorithm = HashAlgorithm::Crc64;
    uploadOptions.TransactionalContentHash.Value().Value = Core::Convert::Base64Decode(DummyCrc64);
    EXPECT_NO_THROW(blobClient.AppendBlock(bodyStream, uploadOptions));

    reset();
    uploadOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::Md5;
    EXPECT_NO_THROW(blobClient.AppendBlock(bodyStream, uploadOptions));
    EXPECT_THROW(
        blobClientWithModifiedContentHash.AppendBlock(bodyStream, uploadOptions), StorageException);

    reset();
    uploadOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::Md5;
    uploadOptions.TransferValidation.PrecalculatedChecksum = md5;
    EXPECT_NO_THROW(blobClient.AppendBlock(bodyStream, uploadOptions));
    EXPECT_THROW(
        blobClientWithModifiedContentHash.AppendBlock(bodyStream, uploadOptions), StorageException);

    reset();
    uploadOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::Md5;
    uploadOptions.TransferValidation.PrecalculatedChecksum = Core::Convert::Base64Decode(DummyMd5);
    EXPECT_THROW(blobClient.AppendBlock(bodyStream, uploadOptions), StorageException);

    reset();
    uploadOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::StorageCrc64;
    EXPECT_NO_THROW(blobClient.AppendBlock(bodyStream, uploadOptions));
    EXPECT_THROW(
        blobClientWithModifiedContentHash.AppendBlock(bodyStream, uploadOptions), StorageException);

    reset();
    uploadOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::StorageCrc64;
    uploadOptions.TransferValidation.PrecalculatedChecksum = crc64;
    EXPECT_NO_THROW(blobClient.AppendBlock(bodyStream, uploadOptions));
    EXPECT_THROW(
        blobClientWithModifiedContentHash.AppendBlock(bodyStream, uploadOptions), StorageException);

    reset();
    uploadOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::StorageCrc64;
    uploadOptions.TransferValidation.PrecalculatedChecksum
        = Core::Convert::Base64Decode(DummyCrc64);
    EXPECT_THROW(blobClient.AppendBlock(bodyStream, uploadOptions), StorageException);
  }

  TEST_F(PageBlobClientTest, UploadPagesTransferValidation)
  {
    auto blobClient = GetPageBlobClient("aaaa");
    blobClient.Create(512);
    auto buffer = RandomBuffer(512);
    Core::Cryptography::Md5Hash md5Hasher;
    md5Hasher.Append(buffer.data(), buffer.size());
    auto md5 = md5Hasher.Final();

    Crc64Hash crc64Hasher;
    crc64Hasher.Append(buffer.data(), buffer.size());
    auto crc64 = crc64Hasher.Final();
    Core::IO::MemoryBodyStream bodyStream(buffer.data(), buffer.size());

    auto clientOptions = Blobs::BlobClientOptions();
    clientOptions.PerOperationPolicies.push_back(
        std::make_unique<ModifyTransactioalHashHeaderPolicy>());
    auto blobClientWithModifiedContentHash
        = Blobs::PageBlobClient(blobClient.GetUrl() + GetSas(), clientOptions);

    Blobs::UploadPagesOptions uploadOptions;

    auto reset = [&]() {
      bodyStream.Rewind();
      uploadOptions = Blobs::UploadPagesOptions();
    };

    reset();
    uploadOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::Auto;
    EXPECT_NO_THROW(blobClient.UploadPages(0, bodyStream, uploadOptions));

    reset();
    uploadOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::Auto;
    uploadOptions.TransactionalContentHash = ContentHash();
    uploadOptions.TransactionalContentHash.Value().Algorithm = HashAlgorithm::Md5;
    uploadOptions.TransactionalContentHash.Value().Value = Core::Convert::Base64Decode(DummyMd5);
    EXPECT_THROW(blobClient.UploadPages(0, bodyStream, uploadOptions), StorageException);

    reset();
    uploadOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::Auto;
    uploadOptions.TransactionalContentHash = ContentHash();
    uploadOptions.TransactionalContentHash.Value().Algorithm = HashAlgorithm::Crc64;
    uploadOptions.TransactionalContentHash.Value().Value = Core::Convert::Base64Decode(DummyCrc64);
    EXPECT_THROW(blobClient.UploadPages(0, bodyStream, uploadOptions), StorageException);

    reset();
    uploadOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::None;
    EXPECT_NO_THROW(blobClient.UploadPages(0, bodyStream, uploadOptions));

    reset();
    uploadOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::None;
    uploadOptions.TransferValidation.PrecalculatedChecksum = Core::Convert::Base64Decode(DummyMd5);
    EXPECT_NO_THROW(blobClient.UploadPages(0, bodyStream, uploadOptions));

    reset();
    uploadOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::None;
    uploadOptions.TransactionalContentHash = ContentHash();
    uploadOptions.TransactionalContentHash.Value().Algorithm = HashAlgorithm::Md5;
    uploadOptions.TransactionalContentHash.Value().Value = Core::Convert::Base64Decode(DummyMd5);
    EXPECT_NO_THROW(blobClient.UploadPages(0, bodyStream, uploadOptions));

    reset();
    uploadOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::None;
    uploadOptions.TransactionalContentHash = ContentHash();
    uploadOptions.TransactionalContentHash.Value().Algorithm = HashAlgorithm::Crc64;
    uploadOptions.TransactionalContentHash.Value().Value = Core::Convert::Base64Decode(DummyCrc64);
    EXPECT_NO_THROW(blobClient.UploadPages(0, bodyStream, uploadOptions));

    reset();
    uploadOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::Md5;
    EXPECT_NO_THROW(blobClient.UploadPages(0, bodyStream, uploadOptions));
    EXPECT_THROW(
        blobClientWithModifiedContentHash.UploadPages(0, bodyStream, uploadOptions),
        StorageException);

    reset();
    uploadOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::Md5;
    uploadOptions.TransferValidation.PrecalculatedChecksum = md5;
    EXPECT_NO_THROW(blobClient.UploadPages(0, bodyStream, uploadOptions));
    EXPECT_THROW(
        blobClientWithModifiedContentHash.UploadPages(0, bodyStream, uploadOptions),
        StorageException);

    reset();
    uploadOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::Md5;
    uploadOptions.TransferValidation.PrecalculatedChecksum = Core::Convert::Base64Decode(DummyMd5);
    EXPECT_THROW(blobClient.UploadPages(0, bodyStream, uploadOptions), StorageException);

    reset();
    uploadOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::StorageCrc64;
    EXPECT_NO_THROW(blobClient.UploadPages(0, bodyStream, uploadOptions));
    EXPECT_THROW(
        blobClientWithModifiedContentHash.UploadPages(0, bodyStream, uploadOptions),
        StorageException);

    reset();
    uploadOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::StorageCrc64;
    uploadOptions.TransferValidation.PrecalculatedChecksum = crc64;
    EXPECT_NO_THROW(blobClient.UploadPages(0, bodyStream, uploadOptions));
    EXPECT_THROW(
        blobClientWithModifiedContentHash.UploadPages(0, bodyStream, uploadOptions),
        StorageException);

    reset();
    uploadOptions.TransferValidation.ChecksumAlgorithm = StorageChecksumAlgorithm::StorageCrc64;
    uploadOptions.TransferValidation.PrecalculatedChecksum
        = Core::Convert::Base64Decode(DummyCrc64);
    EXPECT_THROW(blobClient.UploadPages(0, bodyStream, uploadOptions), StorageException);
  }

}}} // namespace Azure::Storage::Test