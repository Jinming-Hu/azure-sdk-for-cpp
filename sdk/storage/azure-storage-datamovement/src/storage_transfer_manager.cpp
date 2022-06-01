// Copyright (c) Microsoft Corporation. All rights reserved.
// SPDX-License-Identifier: MIT

#include "azure/storage/datamovement/storage_transfer_manager.hpp"

#include <atomic>
#include <fstream>
#include <mutex>
#include <stdexcept>
#include <streambuf>

#include <azure/core/uuid.hpp>

#include "azure/storage/datamovement/task_serialization.hpp"

namespace Azure { namespace Storage {

  namespace _internal {
    void ToSerializationObject(SerializationObject& object, const JobDetails& value)
    {
      object["job_id"] = value.Id;
      object["source_url"] = value.SourceUrl;
      object["destination_url"] = value.DestinationUrl;
      object["transfer_type"] = value.Type;
      object["shared_task_status"] = value.SharedStatus.lock();
    }
    void FromSerializationObject(const SerializationObject& object, JobDetails& value)
    {
      value.Id = object["job_id"].Get<std::string>();
      value.SourceUrl = object["source_url"].Get<std::string>();
      value.DestinationUrl = object["destination_url"].Get<std::string>();
      value.Type = object["transfer_type"].Get<decltype(value.Type)>();
      value.SharedStatus = object["shared_task_status"].Get<std::shared_ptr<TaskSharedStatus>>();
    }
  } // namespace _internal

  _internal::SchedulerOptions StorageTransferManager::GetSchedulerOptions()
  {
    _internal::SchedulerOptions options;
    options.NumThreads = m_options.NumThreads;
    options.MaxMemorySize = m_options.MaxMemorySize;

    struct SerializedJobWriter
    {
      std::string Path;
      _internal::SerializationObject SerializedJob
          = _internal::SerializationObject::CreateRootObject();
      SerializedJobWriter() = default;
      SerializedJobWriter(const SerializedJobWriter&) = delete;
      SerializedJobWriter(SerializedJobWriter&& other) noexcept
      {
        Path = std::move(other.Path);
        other.Path.clear();
        SerializedJob = std::move(other.SerializedJob);
      }
      SerializedJobWriter& operator=(const SerializedJobWriter&) = delete;
      SerializedJobWriter& operator=(SerializedJobWriter&&) = delete;
      ~SerializedJobWriter()
      {
        if (!Path.empty())
        {
          SerializedJob["shared_pointers"] = SerializedJob.Context->SharedObjects;
          auto jobString = SerializedJob.ToString();
          std::ofstream fout(Path, std::ofstream::binary);
          fout.write(jobString.data(), jobString.length());
          fout.close();
        }
        if (SerializedJob.Context)
        {
          SerializedJob.Context->SharedObjects = nullptr;
          SerializedJob.Context->SharedObjects.Context.reset();
        }
      }
    };
    auto serializedJobs = std::make_shared<std::map<std::string, SerializedJobWriter>>();
    options.SerializeTaskCallback = [this, serializedJobs](_internal::Task& task) {
      auto jobStatus = task->SharedStatus->Status.load(std::memory_order_relaxed);
      if (jobStatus != JobStatus::InProgress && jobStatus != JobStatus::Paused)
      {
        return;
      }
      auto jobId = task->SharedStatus->JobId;
      if (serializedJobs->count(jobId) == 0)
      {
        std::string serializedPath = m_options.TransferStateDirectoryPath + "/" + jobId;
        SerializedJobWriter jobWriter;
        jobWriter.Path = serializedPath;

        if (!jobWriter.SerializedJob.Contains("job_details"))
        {
          std::lock_guard<std::mutex> guard(m_jobDetailsMutex);
          jobWriter.SerializedJob["job_details"] = m_jobDetails[jobId];
        }
        serializedJobs->emplace(jobId, std::move(jobWriter));
      }
      auto& serializedJob = serializedJobs->at(jobId).SerializedJob;
      auto taskObject = serializedJob.GetObject();
      task->Serialize(taskObject);
      serializedJob["tasks"].PushBack(std::move(taskObject));
    };
    return options;
  }

  StorageTransferManager::StorageTransferManager(const StorageTransferManagerOptions& options)
      : m_options(options), m_scheduler(GetSchedulerOptions())
  {
  }

  StorageTransferManager::~StorageTransferManager() { PauseAllJobs(); }

  std::shared_ptr<_internal::TaskSharedStatus> StorageTransferManager::GetJobStatus(
      const std::string& jobId)
  {
    std::lock_guard<std::mutex> guard(m_jobDetailsMutex);
    std::shared_ptr<_internal::TaskSharedStatus> jobStatus;
    auto ite = m_jobDetails.find(jobId);
    if (ite != m_jobDetails.end())
    {
      jobStatus = ite->second.SharedStatus.lock();
    }
    return jobStatus;
  }

  void StorageTransferManager::CancelJob(const std::string& jobId)
  {
    auto jobStatus = GetJobStatus(jobId);
    if (!jobStatus)
    {
      throw std::runtime_error("Cannot find job.");
    }
    auto currStatus = jobStatus->Status.load(std::memory_order_relaxed);
    while (currStatus == JobStatus::InProgress || currStatus == JobStatus::Paused)
    {
      bool successfullyCancelled = jobStatus->Status.compare_exchange_weak(
          currStatus, JobStatus::Cancelled, std::memory_order_relaxed, std::memory_order_relaxed);
      if (successfullyCancelled)
      {
        return;
      }
    }
    if (currStatus == JobStatus::Cancelled)
    {
      return;
    }
    throw std::runtime_error(
        "Failed to cancel job " + jobId + ", current status is "
        + _internal::JobStatusToString(currStatus) + ".");
  }

  void StorageTransferManager::CancelAllJobs()
  {
    std::lock_guard<std::mutex> guard(m_jobDetailsMutex);
    for (auto& p : m_jobDetails)
    {
      auto jobStatus = p.second.SharedStatus.lock();
      if (!jobStatus)
      {
        continue;
      }
      auto currStatus = jobStatus->Status.load(std::memory_order_relaxed);
      while (currStatus == JobStatus::InProgress || currStatus == JobStatus::Paused)
      {
        bool successfullyCancelled = jobStatus->Status.compare_exchange_weak(
            currStatus, JobStatus::Cancelled, std::memory_order_relaxed, std::memory_order_relaxed);
        if (successfullyCancelled)
        {
          break;
        }
      }
    }
  }

  void StorageTransferManager::PauseJob(const std::string& jobId)
  {
    auto jobStatus = GetJobStatus(jobId);
    if (!jobStatus)
    {
      throw std::runtime_error("Cannot find job.");
    }
    auto currStatus = jobStatus->Status.load(std::memory_order_relaxed);
    if (currStatus == JobStatus::InProgress)
    {
      bool successfullyPaused = jobStatus->Status.compare_exchange_strong(
          currStatus, JobStatus::Paused, std::memory_order_relaxed, std::memory_order_relaxed);
      if (successfullyPaused)
      {
        return;
      }
    }
    if (currStatus == JobStatus::Paused)
    {
      return;
    }
    throw std::runtime_error(
        "Failed to pause Job " + jobId + ", current status is "
        + _internal::JobStatusToString(currStatus) + ".");
  }

  void StorageTransferManager::PauseAllJobs()
  {
    std::lock_guard<std::mutex> guard(m_jobDetailsMutex);
    for (auto& p : m_jobDetails)
    {
      auto jobStatus = p.second.SharedStatus.lock();
      if (!jobStatus)
      {
        continue;
      }
      auto currStatus = jobStatus->Status.load(std::memory_order_relaxed);
      if (currStatus == JobStatus::InProgress)
      {
        jobStatus->Status.compare_exchange_strong(
            currStatus, JobStatus::Paused, std::memory_order_relaxed, std::memory_order_relaxed);
      }
    }
  }

  JobProperties StorageTransferManager::ResumeJob(
      const std::string& jobId,
      const ResumeJobOptions& options)
  {
    (void)options;

    auto jobStatus = GetJobStatus(jobId);
    if (jobStatus)
    {
      auto currStatus = JobStatus::Paused;
      bool successfullyResumed = jobStatus->Status.compare_exchange_strong(
          currStatus, JobStatus::InProgress, std::memory_order_relaxed, std::memory_order_relaxed);
      if (!successfullyResumed)
      {
        throw std::runtime_error(
            "Failed to resume job " + jobId + ", current status is "
            + _internal::JobStatusToString(currStatus) + ".");
      }
      m_scheduler.ResumePausedTasks();
      std::lock_guard<std::mutex> guard(m_jobDetailsMutex);
      return m_jobDetails.at(jobId).GetJobProperties();
    }

    // try to resume from journal file
    struct SerializedJobReader
    {
      _internal::SerializationObject SerializedJob
          = _internal::SerializationObject::CreateRootObject();

      explicit SerializedJobReader(const std::string& path)
      {
        std::ifstream fin(path, std::ifstream::binary);
        if (fin.fail())
        {
          throw std::runtime_error("Cannot find job.");
        }
        std::string fileContent(
            (std::istreambuf_iterator<char>(fin)), std::istreambuf_iterator<char>());
        SerializedJob.FromString(fileContent);
        SerializedJob.Context->SharedObjects = SerializedJob["shared_pointers"];
      }
      SerializedJobReader(const SerializedJobReader&) = delete;
      SerializedJobReader& operator=(const SerializedJobReader&) = delete;
      ~SerializedJobReader()
      {
        if (SerializedJob.Context)
        {
          SerializedJob.Context->SharedObjects = nullptr;
          SerializedJob.Context->SharedObjects.Context.reset();
        }
      }
    };

    SerializedJobReader jobReader(m_options.TransferStateDirectoryPath + "/" + jobId);

    std::vector<_internal::Task> tasks;
    for (size_t i = 0; i < jobReader.SerializedJob["tasks"].Size(); ++i)
    {
      tasks.push_back(_internal::DeserializeTask(jobReader.SerializedJob["tasks"][i]));
    }

    auto jobDetails = jobReader.SerializedJob["job_details"].Get<_internal::JobDetails>();
    auto sharedStatus = jobDetails.SharedStatus.lock();
    sharedStatus->Scheduler = &m_scheduler;
    sharedStatus->ProgressHandler = options.ProgressHandler;
    sharedStatus->ErrorHandler = options.ErrorHandler;
    auto currStatus = sharedStatus->Status.load(std::memory_order_relaxed);
    if (currStatus == JobStatus::Paused)
    {
      sharedStatus->Status = JobStatus::InProgress;
    }
    else if (currStatus != JobStatus::InProgress)
    {
      throw std::runtime_error(
          "Failed to resume job " + jobId + ", current status is "
          + _internal::JobStatusToString(currStatus) + ".");
    }

    {
      std::lock_guard<std::mutex> guard(m_jobDetailsMutex);
      auto insertResult = m_jobDetails.emplace(jobDetails.Id, std::move(jobDetails));
      if (!insertResult.second)
      {
        return ResumeJob(jobId, options);
      }
    }
    m_scheduler.AddTasks(std::move(tasks));
    return m_jobDetails.at(jobId).GetJobProperties();
  }

  std::pair<JobProperties, _internal::Task> StorageTransferManager::CreateJob(
      TransferType type,
      std::string sourceUrl,
      std::string destinationUrl)
  {
    struct DummyTask final : public Storage::_internal::TaskBase
    {
      using TaskBase::TaskBase;
      void Execute() noexcept override { AZURE_UNREACHABLE_CODE(); }
      void Serialize(_internal::SerializationObject&) noexcept override
      {
        AZURE_UNREACHABLE_CODE();
      }
      void Deserialize(const _internal::SerializationObject&) noexcept override
      {
        AZURE_UNREACHABLE_CODE();
      }
    };

    auto jobId = Core::Uuid::CreateUuid().ToString();

    auto sharedStatus = std::make_shared<_internal::TaskSharedStatus>();
    sharedStatus->Scheduler = &m_scheduler;
    sharedStatus->JobId = jobId;
    _internal::JobDetails jobDetails;
    jobDetails.Id = jobId;
    jobDetails.SourceUrl = std::move(sourceUrl);
    jobDetails.DestinationUrl = std::move(destinationUrl);
    jobDetails.Type = type;
    jobDetails.SharedStatus = sharedStatus;

    auto jobProperties = jobDetails.GetJobProperties();

    auto rootTask = std::make_unique<DummyTask>(_internal::TaskType::Other);
    rootTask->SharedStatus = sharedStatus;

    {
      std::lock_guard<std::mutex> guard(m_jobDetailsMutex);
      auto insertResult = m_jobDetails.emplace(jobDetails.Id, std::move(jobDetails));
      AZURE_ASSERT(insertResult.second);
    }

    return std::make_pair(std::move(jobProperties), std::move(rootTask));
  }

}} // namespace Azure::Storage
