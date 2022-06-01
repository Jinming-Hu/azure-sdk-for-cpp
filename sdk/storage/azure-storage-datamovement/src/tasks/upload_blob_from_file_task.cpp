// Copyright (c) Microsoft Corporation. All rights reserved.
// SPDX-License-Identifier: MIT

#include "azure/storage/datamovement/tasks/upload_blob_from_file_task.hpp"

#include <algorithm>
#include <limits>
#include <vector>

#include <azure/core/azure_assert.hpp>
#include <azure/core/base64.hpp>

#include "azure/storage/datamovement/job_properties.hpp"
#include "azure/storage/datamovement/scheduler.hpp"
#include "azure/storage/datamovement/task_serialization.hpp"
#include "azure/storage/datamovement/utilities.hpp"

namespace Azure { namespace Storage {

  namespace _internal {
    void ToSerializationObject(
        SerializationObject& object,
        const Blobs::_detail::UploadBlobFromFileTask::TaskContext& value)
    {
      object["source_url"] = value.Source;
      object["destination_url"] = value.Destination.GetUrl();
      object["file_size"] = value.FileSize;
      object["num_blocks"] = value.NumBlocks;
      object["num_staged_blocks"] = value.NumStagedBlocks.load();
      object["failed"] = value.Failed.load();
    }

    void FromSerializationObject(
        const SerializationObject& object,
        Blobs::_detail::UploadBlobFromFileTask::TaskContext& value)
    {
      value.Source = object["source_url"].Get<std::string>();
      value.Destination = Blobs::BlobClient(object["destination_url"].Get<std::string>());
      // TODO: credential
      value.FileSize = object["file_size"].Get<uint64_t>();
      value.NumBlocks = object["num_blocks"].Get<int>();
      value.NumStagedBlocks = object["num_staged_blocks"].Get<int>();
      value.Failed = object["failed"].Get<bool>();
    }
  } // namespace _internal

  namespace Blobs { namespace _detail {

    namespace {
      constexpr uint64_t ChunkSize = 8 * 1024 * 1024;
      static_assert(ChunkSize < static_cast<uint64_t>(std::numeric_limits<size_t>::max()), "");

      std::string GetBlockId(int64_t id)
      {
        // TODO: we want to add identifier in block ID, so that we resuming this job, we can pick up
        // from where we left off. The identifier may include az-storage-dm and the file size
        constexpr size_t BlockIdLength = 64;
        std::string blockId = std::to_string(id);
        blockId = std::string(BlockIdLength - blockId.length(), '0') + blockId;
        return Azure::Core::Convert::Base64Encode(
            std::vector<uint8_t>(blockId.begin(), blockId.end()));
      }

    } // namespace

    void UploadBlobFromFileTask::Execute() noexcept
    {
      if (!Context->FileReader)
      {
        try
        {
          Context->FileReader = std::make_unique<Storage::_internal::FileReader>(Context->Source);
        }
        catch (std::exception&)
        {
          SharedStatus->TaskFailedCallback(
              1, _internal::GetFileUrl(Context->Source), Context->Destination.GetUrl());
          return;
        }
      }
      const uint64_t fileSize = Context->FileReader->GetFileSize();
      Context->FileSize = fileSize;

      if (fileSize == 0)
      {
        Core::IO::MemoryBodyStream emptyStream(nullptr, 0);
        try
        {
          Context->Destination.AsBlockBlobClient().Upload(emptyStream);
        }
        catch (std::exception&)
        {
          SharedStatus->TaskFailedCallback(
              1, _internal::GetFileUrl(Context->Source), Context->Destination.GetUrl());
          return;
        }
        SharedStatus->TaskTransferedCallback(1, fileSize);
        return;
      }

      // TODO: if file is small enough, we'll use single put

      Context->NumBlocks = static_cast<int>((fileSize + ChunkSize - 1) / ChunkSize);
      std::vector<_internal::Task> subtasks;
      for (int blockId = 0; blockId < Context->NumBlocks; ++blockId)
      {
        auto readFileRangeTask = CreateTask<ReadFileRangeToMemoryTask>();
        readFileRangeTask->Context = Context;
        readFileRangeTask->BlockId = blockId;
        readFileRangeTask->Offset = blockId * ChunkSize;
        readFileRangeTask->Length
            = static_cast<size_t>(std::min(ChunkSize, fileSize - blockId * ChunkSize));
        readFileRangeTask->MemoryCost = readFileRangeTask->Length;
        subtasks.push_back(std::move(readFileRangeTask));
      }
      SharedStatus->Scheduler->AddTasks(std::move(subtasks));
    }

    void ReadFileRangeToMemoryTask::Execute() noexcept
    {
      if (Context->Failed.load(std::memory_order_relaxed))
      {
        return;
      }

      std::unique_ptr<uint8_t[]> buffer = std::make_unique<uint8_t[]>(Length);

      try
      {
        size_t bytesRead = Context->FileReader->Read(buffer.get(), Length, Offset);
        if (bytesRead != Length)
        {
          throw std::runtime_error("Failed to read file.");
        }
      }
      catch (std::exception&)
      {
        bool firstFailure = !Context->Failed.exchange(true, std::memory_order_relaxed);
        if (firstFailure)
        {
          SharedStatus->TaskFailedCallback(
              1, _internal::GetFileUrl(Context->Source), Context->Destination.GetUrl());
        }
        return;
      }

      auto stageBlockTask = CreateTask<StageBlockTask>();
      stageBlockTask->Context = Context;
      stageBlockTask->BlockId = BlockId;
      stageBlockTask->Buffer = std::move(buffer);
      stageBlockTask->Offset = Offset;
      stageBlockTask->Length = Length;
      std::swap(stageBlockTask->MemoryGiveBack, this->MemoryGiveBack);

      SharedStatus->Scheduler->AddTask(std::move(stageBlockTask));
    }

    void StageBlockTask::Execute() noexcept
    {
      if (Context->Failed.load(std::memory_order_relaxed))
      {
        return;
      }

      const std::string blockId = GetBlockId(BlockId);
      Core::IO::MemoryBodyStream contentStream(Buffer.get(), Length);
      auto blockBlobClient = Context->Destination.AsBlockBlobClient();
      try
      {
        blockBlobClient.StageBlock(blockId, contentStream);
      }
      catch (std::exception&)
      {
        Buffer.reset();
        bool firstFailure = !Context->Failed.exchange(true, std::memory_order_relaxed);
        if (firstFailure)
        {
          SharedStatus->TaskFailedCallback(
              1, _internal::GetFileUrl(Context->Source), Context->Destination.GetUrl());
        }
        return;
      }

      Buffer.reset();

      int numStagedBlocks = Context->NumStagedBlocks.fetch_add(1, std::memory_order_relaxed) + 1;
      if (numStagedBlocks != Context->NumBlocks)
      {
        return;
      }
      std::vector<std::string> blockIds;
      for (int i = 0; i < Context->NumBlocks; ++i)
      {
        blockIds.push_back(GetBlockId(i));
      }
      try
      {
        blockBlobClient.CommitBlockList(blockIds);
      }
      catch (std::exception&)
      {
        bool firstFailure = !Context->Failed.exchange(true, std::memory_order_relaxed);
        if (firstFailure)
        {
          SharedStatus->TaskFailedCallback(
              1, _internal::GetFileUrl(Context->Source), Context->Destination.GetUrl());
        }
        return;
      }
      SharedStatus->TaskTransferedCallback(1, Context->FileSize);
    }

    void UploadBlobFromFileTask::Serialize(_internal::SerializationObject& object) noexcept
    {
      TaskBase::Serialize<UploadBlobFromFileTask>(object);
      object["context"] = Context;
    }

    void UploadBlobFromFileTask::Deserialize(const _internal::SerializationObject& object) noexcept
    {
      TaskBase::Deserialize(object);
      Context = object["context"].Get<decltype(Context)>();
    }

    void ReadFileRangeToMemoryTask::Serialize(_internal::SerializationObject& object) noexcept
    {
      TaskBase::Serialize<ReadFileRangeToMemoryTask>(object);
      object["_task_name"] = std::string(TaskName);
      object["context"] = Context;
      object["block_id"] = BlockId;
      object["offset"] = Offset;
      object["length"] = Length;
    }

    void ReadFileRangeToMemoryTask::Deserialize(
        const _internal::SerializationObject& object) noexcept
    {
      TaskBase::Deserialize(object);
      Context = object["context"].Get<decltype(Context)>();
      BlockId = object["block_id"].Get<decltype(BlockId)>();
      Offset = object["offset"].Get<decltype(Offset)>();
      Length = object["length"].Get<decltype(Length)>();
    }

    void StageBlockTask::Serialize(_internal::SerializationObject& object) noexcept
    {
      auto task = CreateTask<ReadFileRangeToMemoryTask>();
      task->Context = Context;
      task->BlockId = BlockId;
      task->Offset = Offset;
      task->Length = Length;
      task->MemoryCost = MemoryGiveBack;
      return task->Serialize(object);
    }

  }} // namespace Blobs::_detail
}} // namespace Azure::Storage
