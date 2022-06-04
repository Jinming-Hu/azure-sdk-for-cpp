// Copyright (c) Microsoft Corporation. All rights reserved.
// SPDX-License-Identifier: MIT

#pragma once

#include <map>
#include <mutex>
#include <string>
#include <unordered_set>
#include <vector>

#include <azure/core/nullable.hpp>

namespace Azure { namespace Storage { namespace _internal {
  enum class FilterType
  {
    Bitmap,
    Set,
    Lexical,
    Mono,
  };

  class Filter {
  public:
    Filter(const Filter&) = delete;
    Filter& operator=(const Filter&) = delete;
    Filter(Filter&& other) noexcept;
    ~Filter();

    bool IsBitmapFiltered(size_t index);
    void SetBitmap(size_t index);
    bool IsSetFiltered(const std::string& value);
    void AddToSet(std::string value);
    bool IsLexicalFiltered(const std::string& value);
    void SetLexical(std::string value);

  private:
    Filter() : m_disengaged{} {}

    union
    {
      Azure::_detail::NontrivialEmptyType m_disengaged;
      std::vector<bool> m_bitmap;
      std::unordered_set<std::string> m_set;
      std::string m_str;
    };
    std::mutex m_mutex;
    FilterType m_type = static_cast<FilterType>(0);

    friend class TaskFilters;
  };

  class TaskFilters {
  public:
    std::shared_ptr<Filter> CreateBitmapFilter(const std::string& name, size_t size);
    std::shared_ptr<Filter> CreateSetFilter(const std::string& name);
    std::shared_ptr<Filter> CreateLexicalFilter(const std::string& name);
    std::shared_ptr<Filter> CreateMonoFilter(const std::string& name);

    bool HasFilter(const std::string& name);
    void DeleteFilter(const std::string& name);

    bool IsFiltered(const std::string& name, size_t index);
    bool IsFiltered(const std::string& name, const std::string& value);


  private:
    std::map<std::string, std::shared_ptr<Filter>> m_filters;
    std::mutex m_filtersMutex;
  };

}}} // namespace Azure::Storage::_internal
