// Copyright (c) Microsoft Corporation. All rights reserved.
// SPDX-License-Identifier: MIT

#include "azure/storage/datamovement/storage_transfer_manager.hpp"

#include <atomic>
#include <cstdio>
#include <fstream>
#include <mutex>
#include <stdexcept>
#include <type_traits>

#include <azure/core/internal/json/json.hpp>
#include <azure/core/uuid.hpp>

#include "azure/storage/datamovement/journal.hpp"

namespace Azure { namespace Storage {

  StorageTransferManager::StorageTransferManager(const StorageTransferManagerOptions& options)
      : m_options(options),
        m_scheduler(_internal::SchedulerOptions{options.NumThreads, options.MaxMemorySize})

  {
  }

  StorageTransferManager::~StorageTransferManager()
  {
    m_scheduler.Stop();
    if (!m_options.TransferStateDirectoryPath.empty())
    {
      std::lock_guard<std::mutex> guard(m_jobDetailsMutex);
      for (auto& pair : m_jobDetails)
      {
        auto& jobDetails = pair.second;
        auto sharedStatus = jobDetails.SharedStatus.lock();
        if (!sharedStatus)
        {
          continue;
        }
        auto jobStatus = sharedStatus->Status.load(std::memory_order_relaxed);
        if (jobStatus != JobStatus::InProgress && jobStatus != JobStatus::Paused)
        {
          continue;
        }
        Core::Json::_internal::json serializedObject;
        serializedObject["id"] = jobDetails.Id;
        serializedObject["source_url"] = jobDetails.SourceUrl;
        serializedObject["destination_url"] = jobDetails.DestinationUrl;
        serializedObject["type"]
            = static_cast<std::underlying_type_t<TransferType>>(jobDetails.Type);
        serializedObject["shared_status"] = {};
        serializedObject["shared_status"]["num_files_transferred"]
            = sharedStatus->NumFilesTransferred.load(std::memory_order_relaxed);
        serializedObject["shared_status"]["num_files_skipped"]
            = sharedStatus->NumFilesSkipped.load(std::memory_order_relaxed);
        serializedObject["shared_status"]["num_files_failed"]
            = sharedStatus->NumFilesFailed.load(std::memory_order_relaxed);
        serializedObject["shared_status"]["total_bytes_transferred"]
            = sharedStatus->TotalBytesTransferred.load(std::memory_order_relaxed);
        serializedObject["journal"] = jobDetails.JournalTree->ToString();
        const std::string journalFileName
            = m_options.TransferStateDirectoryPath + "/" + jobDetails.Id;
        std::ofstream fout(journalFileName + ".tmp", std::ofstream::binary);
        std::string fileContent = serializedObject.dump(4);
        fout.write(fileContent.data(), fileContent.size());
        fout.close();
        int ret = std::rename((journalFileName + ".tmp").data(), journalFileName.data());
        (void)ret;
      }
    }
  }

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
    if (!jobStatus)
    {
      // TODO: also support resume from journal file
      throw std::runtime_error("Cannot find job.");
    }
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

  std::pair<JobProperties, _internal::Task> StorageTransferManager::CreateJob(
      TransferType type,
      std::string sourceUrl,
      std::string destinationUrl)
  {
    struct DummyTask final : public Storage::_internal::TaskBase
    {
      DummyTask() : TaskBase(_internal::TaskType::Other) {}
      void Execute() noexcept override { AZURE_UNREACHABLE_CODE(); }
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
    bool journalEnabled = !m_options.TransferStateDirectoryPath.empty();
    if (journalEnabled)
    {
      jobDetails.JournalTree = std::make_unique<_internal::JournalTree>();
    }

    auto jobProperties = jobDetails.GetJobProperties();

    auto rootTask = std::make_unique<DummyTask>();
    rootTask->SharedStatus = sharedStatus;
    if (journalEnabled)
    {
      rootTask->JournalAgent = _internal::JournalAgent(*jobDetails.JournalTree);
    }

    {
      std::lock_guard<std::mutex> guard(m_jobDetailsMutex);
      auto insertResult = m_jobDetails.emplace(jobDetails.Id, std::move(jobDetails));
      AZURE_ASSERT(insertResult.second);
    }

    return std::make_pair(std::move(jobProperties), std::move(rootTask));
  }

}} // namespace Azure::Storage
