// Copyright (c) Microsoft Corporation. All rights reserved.
// SPDX-License-Identifier: MIT

#include "azure/storage/datamovement/task.hpp"

#include <type_traits>

#include "azure/storage/datamovement/job_properties.hpp"
#include "azure/storage/datamovement/task_serialization.hpp"
#include "azure/storage/datamovement/tasks/upload_blob_from_file_task.hpp"

namespace Azure { namespace Storage { namespace _internal {

  void TaskBase::Deserialize(const SerializationObject& object) noexcept
  {
    Type = object["_task_type"].Get<decltype(Type)>();
    SharedStatus = object["_task_shared_status"].Get<decltype(SharedStatus)>();
    MemoryCost = object["_memory_cost"].Get<decltype(MemoryCost)>();
  }

  namespace {
    template <class T>
    std::pair<std::string, std::function<std::unique_ptr<TaskBase>()>> RegisterTask()
    {
      return std::make_pair(T::TaskName, []() { return std::make_unique<T>(); });
    }
  } // namespace

  Task DeserializeTask(const SerializationObject& object)
  {
    static const std::map<std::string, std::function<std::unique_ptr<TaskBase>()>> taskFactories = {
        RegisterTask<Blobs::_detail::UploadBlobFromFileTask>(),
        RegisterTask<Blobs::_detail::ReadFileRangeToMemoryTask>(),
    };

    const auto task_name = object["_task_name"].Get<std::string>();
    Task task = taskFactories.at(task_name)();
    task->Deserialize(object);
    return task;
  }

}}} // namespace Azure::Storage::_internal
