// Copyright (c) Microsoft Corporation. All rights reserved.
// SPDX-License-Identifier: MIT

#include "azure/storage/datamovement/task_filter.hpp"

namespace Azure { namespace Storage { namespace _internal {

  Filter::Filter(Filter&& other) noexcept : m_disengaged{}
  {
    m_type = other.m_type;
    if (m_type == FilterType::Bitmap)
    {
      ::new (&m_bitmap) std::vector<bool>(std::move(other.m_bitmap));
    }
    else if (m_type == FilterType::Set)
    {
      ::new (&m_set) std::unordered_set<std::string>(std::move(other.m_set));
    }
    else if (m_type == FilterType::Lexical)
    {
      ::new (&m_str) std::string(std::move(other.m_str));
    }
  }

  Filter::~Filter()
  {
    if (m_type == FilterType::Bitmap)
    {
      m_bitmap.std::vector<bool>::~vector<bool>();
    }
    else if (m_type == FilterType::Set)
    {
      m_set.std::unordered_set<std::string>::~unordered_set<std::string>();
    }
    else if (m_type == FilterType::Lexical)
    {
      m_str.std::string::~string();
    }
  }

  bool Filter::IsBitmapFiltered(size_t index)
  {
    if (m_type != FilterType::Bitmap)
    {
      AZURE_UNREACHABLE_CODE();
    }
    std::lock_guard<std::mutex> guard(m_mutex);
    return m_bitmap[index];
  }

  void Filter::SetBitmap(size_t index)
  {
    if (m_type != FilterType::Bitmap)
    {
      AZURE_UNREACHABLE_CODE();
    }
    std::lock_guard<std::mutex> guard(m_mutex);
    m_bitmap[index] = true;
  }

  bool Filter::IsSetFiltered(const std::string& value)
  {
    if (m_type != FilterType::Set)
    {
      AZURE_UNREACHABLE_CODE();
    }
    std::lock_guard<std::mutex> guard(m_mutex);
    return m_set.count(value) != 0;
  }

  void Filter::AddToSet(std::string value)
  {
    if (m_type != FilterType::Set)
    {
      AZURE_UNREACHABLE_CODE();
    }
    std::lock_guard<std::mutex> guard(m_mutex);
    m_set.emplace(std::move(value));
  }

  bool Filter::IsLexicalFiltered(const std::string& value)
  {
    if (m_type != FilterType::Lexical)
    {
      AZURE_UNREACHABLE_CODE();
    }
    std::lock_guard<std::mutex> guard(m_mutex);
    return value < m_str;
  }

  void Filter::SetLexical(std::string value)
  {
    if (m_type != FilterType::Lexical)
    {
      AZURE_UNREACHABLE_CODE();
    }
    std::lock_guard<std::mutex> guard(m_mutex);
    m_str = std::move(value);
  }

  std::shared_ptr<Filter> TaskFilters::CreateBitmapFilter(const std::string& name, size_t size)
  {
    Filter f;
    f.m_type = FilterType::Bitmap;
    ::new (&f.m_bitmap) std::vector<bool>(size);

    std::lock_guard<std::mutex> guard(m_filtersMutex);
    auto ret = m_filters.emplace(name, std::make_shared<Filter>(std::move(f)));
    return ret.first->second;
  }

  std::shared_ptr<Filter> TaskFilters::CreateSetFilter(const std::string& name)
  {
    Filter f;
    f.m_type = FilterType::Set;
    ::new (&f.m_set) std::unordered_set<std::string>();

    std::lock_guard<std::mutex> guard(m_filtersMutex);
    auto ret = m_filters.emplace(name, std::make_shared<Filter>(std::move(f)));
    return ret.first->second;
  }

  std::shared_ptr<Filter> TaskFilters::CreateLexicalFilter(const std::string& name)
  {
    Filter f;
    f.m_type = FilterType::Lexical;
    ::new (&f.m_str) std::string();

    std::lock_guard<std::mutex> guard(m_filtersMutex);
    auto ret = m_filters.emplace(name, std::make_shared<Filter>(std::move(f)));
    return ret.first->second;
  }

  std::shared_ptr<Filter> TaskFilters::CreateMonoFilter(const std::string& name)
  {
    Filter f;
    f.m_type = FilterType::Mono;

    std::lock_guard<std::mutex> guard(m_filtersMutex);
    auto ret = m_filters.emplace(name, std::make_shared<Filter>(std::move(f)));
    return ret.first->second;
  }

  bool TaskFilters::HasFilter(const std::string& name)
  {
    std::lock_guard<std::mutex> guard(m_filtersMutex);
    return m_filters.count(name) != 0;
  }

  std::shared_ptr<Filter> TaskFilters::GetFilter(const std::string& name)
  {
    std::lock_guard<std::mutex> guard(m_filtersMutex);
    auto ite = m_filters.find(name);
    if (ite != m_filters.end())
    {
      return ite->second;
    }
  }

  void TaskFilters::DeleteFilter(const std::string& name)
  {
    std::lock_guard<std::mutex> guard(m_filtersMutex);
    m_filters.erase(name);
  }

}}} // namespace Azure::Storage::_internal