// Copyright (c) Microsoft Corporation. All rights reserved.
// SPDX-License-Identifier: MIT

#include "azure/storage/datamovement/task_serialization.hpp"

#include <azure/core/internal/json/json.hpp>

#include "azure/storage/datamovement/job_properties.hpp"

namespace Azure { namespace Storage { namespace _internal {

  SerializationObject SerializationObject::CreateRootObject()
  {
    auto context = std::make_shared<SerializationContext>(SerializationContext());
    context->SharedObjects.Context = context;
    return context->SharedObjects.GetObject();
  }

  SerializationObject SerializationObject::GetObject()
  {
    SerializationObject obj;
    obj.Context = Context;
    return obj;
  }

  std::string SerializationObject::ToString() const
  {
    std::function<Core::Json::_internal::json(const SerializationObject&)> toJson;
    toJson = [&toJson](const SerializationObject& object) -> Core::Json::_internal::json {
      if (object.Type == SerializationVariableType::Null)
      {
        return std::nullptr_t();
      }
      else if (object.Type == SerializationVariableType::Int)
      {
        return object.m_int;
      }
      else if (object.Type == SerializationVariableType::Uint)
      {
        return object.m_uint;
      }
      else if (object.Type == SerializationVariableType::Bool)
      {
        return object.m_int ? true : false;
      }
      else if (object.Type == SerializationVariableType::String)
      {
        return object.m_string;
      }
      else if (object.Type == SerializationVariableType::Array)
      {
        auto arr = Core::Json::_internal::json::array();
        for (auto& i : object.m_array)
        {
          arr.push_back(toJson(i));
        }
        return arr;
      }
      else if (object.Type == SerializationVariableType::Dict)
      {
        auto obj = Core::Json::_internal::json::object();
        for (auto& i : object.m_dict)
        {
          obj[i.first] = toJson(i.second);
        }
        return obj;
      }
      else
      {
        throw std::runtime_error("Unknown serialization object type.");
      }
    };
    return toJson(*this).dump(4);
  }

  void SerializationObject::FromString(const std::string& str)
  {
    std::function<SerializationObject(const Core::Json::_internal::json&)> fromJson;
    fromJson = [&fromJson, this](const Core::Json::_internal::json& json) -> SerializationObject {
      SerializationObject object = GetObject();
      if (json.is_null())
      {
        object = nullptr;
      }
      else if (json.is_number_integer())
      {
        object = json.get<int64_t>();
      }
      else if (json.is_number_unsigned())
      {
        object = json.get<uint64_t>();
      }
      else if (json.is_boolean())
      {
        object = json.get<bool>();
      }
      else if (json.is_string())
      {
        object = json.get<std::string>();
      }
      else if (json.is_array())
      {
        for (auto& i : json)
        {
          object.PushBack(fromJson(i));
        }
      }
      else if (json.is_object())
      {
        for (auto p : json.items())
        {
          object[p.key()] = fromJson(p.value());
        }
      }
      else
      {
        throw std::runtime_error("Unknown json object.");
      }
      return object;
    };
    *this = fromJson(Core::Json::_internal::json::parse(str));
  }

  size_t SerializationObject::Size() const
  {
    if (Type == SerializationVariableType::Array)
    {
      return m_array.size();
    }
    if (Type == SerializationVariableType::Dict)
    {
      return m_dict.size();
    }
    if (Type == SerializationVariableType::Null)
    {
      return 0; 
    }
    throw std::runtime_error("Not an array or dictionary.");
  }

  bool SerializationObject::Contains(const std::string& key) const
  {
    if (Type == SerializationVariableType::Null)
    {
      return false;
    }
    if (Type != SerializationVariableType::Dict)
    {
      throw std::runtime_error("Not a dictionary.");
    }
    return m_dict.count(key) != 0;
  }

  SerializationObject& SerializationObject::operator[](const std::string& key)
  {
    if (Type == SerializationVariableType::Null)
    {
      Type = SerializationVariableType::Dict;
    }
    if (Type != SerializationVariableType::Dict)
    {
      throw std::runtime_error("Not a dictionary.");
    }
    auto ite = m_dict.find(key);
    if (ite == m_dict.end())
    {
      auto obj = GetObject();
      ite = m_dict.emplace(key, std::move(obj)).first;
    }
    return ite->second;
  }

  const SerializationObject& SerializationObject::operator[](const std::string& key) const
  {
    if (Type != SerializationVariableType::Dict)
    {
      throw std::runtime_error("Not a dictionary.");
    }
    return m_dict.at(key);
  }

  SerializationObject& SerializationObject::operator[](size_t index)
  {
    if (Type == SerializationVariableType::Null)
    {
      Type = SerializationVariableType::Array;
    }
    if (Type != SerializationVariableType::Array)
    {
      throw std::runtime_error("Not an array.");
    }
    return m_array.at(index);
  }

  const SerializationObject& SerializationObject::operator[](size_t index) const
  {
    if (Type != SerializationVariableType::Array)
    {
      throw std::runtime_error("Not an array.");
    }
    return m_array.at(index);
  }

  void SerializationObject::PushBack(SerializationObject object)
  {
    if (Type == SerializationVariableType::Null)
    {
      Type = SerializationVariableType::Array;
    }
    if (Type != SerializationVariableType::Array)
    {
      throw std::runtime_error("Not an array.");
    }
    m_array.push_back(std::move(object));
  }

  void ToSerializationObject(SerializationObject& object, const TaskSharedStatus& value)
  {
    object["job_id"] = value.JobId;
    object["job_status"] = value.Status.load();
    object["num_files_transferred"] = value.NumFilesTransferred.load();
    object["num_files_skipped"] = value.NumFilesSkipped.load();
    object["num_files_failed"] = value.NumFilesFailed.load();
    object["total_bytes_transferred"] = value.TotalBytesTransferred.load();
  }

  void FromSerializationObject(const SerializationObject& object, TaskSharedStatus& value)
  {
    value.JobId = object["job_id"].Get<std::string>();
    value.Status = object["job_status"].Get<JobStatus>();
    value.NumFilesTransferred = object["num_files_transferred"].Get<int64_t>();
    value.NumFilesSkipped = object["num_files_skipped"].Get<int64_t>();
    value.NumFilesFailed = object["num_files_failed"].Get<int64_t>();
    value.TotalBytesTransferred = object["total_bytes_transferred"].Get<int64_t>();
  }

}}} // namespace Azure::Storage::_internal
