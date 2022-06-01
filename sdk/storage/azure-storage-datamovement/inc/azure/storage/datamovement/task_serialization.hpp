// Copyright (c) Microsoft Corporation. All rights reserved.
// SPDX-License-Identifier: MIT

#pragma once

#include <map>
#include <memory>
#include <string>
#include <type_traits>
#include <vector>

#include <azure/core/azure_assert.hpp>

#include "azure/storage/datamovement/task.hpp"

namespace Azure { namespace Storage {

  namespace _detail {
    template <class T> struct is_shared_ptr : std::false_type
    {
    };
    template <class T> struct is_shared_ptr<std::shared_ptr<T>> : std::true_type
    {
    };
    template <class T> constexpr bool is_shared_ptr_v = is_shared_ptr<T>::value;

    template <class T> struct is_vector : std::false_type
    {
    };
    template <class T> struct is_vector<std::vector<T>> : std::true_type
    {
    };
    template <class T> constexpr bool is_vector_v = is_vector<T>::value;
  } // namespace _detail

  namespace _internal {
    enum class SerializationVariableType
    {
      Null,
      Int,
      Uint,
      Bool,
      String,
      Array,
      Dict,
    };

    struct SerializationObject
    {
      static SerializationObject CreateRootObject();
      SerializationObject GetObject();
      std::string ToString() const;
      void FromString(const std::string& str);

      size_t Size() const;
      bool Contains(const std::string& key) const;
      SerializationObject& operator[](const std::string& key);
      const SerializationObject& operator[](const std::string& key) const;
      SerializationObject& operator[](size_t index);
      const SerializationObject& operator[](size_t index) const;
      void PushBack(SerializationObject object);
      template <class T> void operator=(const T& value) { ToSerializationObject(*this, value); }
      template <class T> T Get() const
      {
        T t;
        FromSerializationObject(*this, t);
        return t;
      }

      SerializationVariableType Type = SerializationVariableType::Null;
      std::shared_ptr<SerializationContext> Context;

    private:
      SerializationObject() = default;

      int64_t m_int{};
      uint64_t m_uint{};
      std::string m_string;
      std::vector<SerializationObject> m_array;
      std::map<std::string, SerializationObject> m_dict;

      friend struct SerializationContext;
      friend void ToSerializationObject(SerializationObject&, const std::nullptr_t&);
      friend void ToSerializationObject(SerializationObject&, const int32_t&);
      friend void ToSerializationObject(SerializationObject&, const uint32_t&);
      friend void ToSerializationObject(SerializationObject&, const int64_t&);
      friend void ToSerializationObject(SerializationObject&, const uint64_t&);
      friend void ToSerializationObject(SerializationObject&, const bool&);
      friend void ToSerializationObject(SerializationObject&, const std::string&);
      friend void ToSerializationObject(
          SerializationObject& object,
          const SerializationObject& other);
      friend void FromSerializationObject(const SerializationObject&, int32_t&);
      friend void FromSerializationObject(const SerializationObject&, uint32_t&);
      friend void FromSerializationObject(const SerializationObject&, int64_t&);
      friend void FromSerializationObject(const SerializationObject&, uint64_t&);
      friend void FromSerializationObject(const SerializationObject&, bool&);
      friend void FromSerializationObject(const SerializationObject&, std::string&);
    };

    struct SerializationContext
    {
      // TODO: destruct of this
      SerializationContext(SerializationContext&&) = default;

      SerializationObject SharedObjects;

    private:
      SerializationContext() { SharedObjects.Type = SerializationVariableType::Array; }

      std::map<void*, size_t> m_sharedPointerMap;
      std::vector<std::weak_ptr<void>> m_sharedPointerVector;

      friend struct SerializationObject;
      template <class T>
      friend typename std::enable_if_t<_detail::is_shared_ptr_v<T>, void> ToSerializationObject(
          SerializationObject& object,
          const T& value);
      template <class T>
      friend typename std::enable_if_t<_detail::is_shared_ptr_v<T>, void> FromSerializationObject(
          const SerializationObject& object,
          T& value);
    };

    template <class T>
    typename std::enable_if_t<
        !_detail::is_shared_ptr_v<T> && !std::is_pointer<T>::value
            && !_detail::is_vector_v<T> && !std::is_enum<T>::value,
        void>
    ToSerializationObject(SerializationObject& object, const T& value);

    inline void ToSerializationObject(SerializationObject& object, const std::nullptr_t&)
    {
      object.Type = SerializationVariableType::Null;
    }

    inline void ToSerializationObject(SerializationObject& object, const int64_t& value)
    {
      object.Type = SerializationVariableType::Int;
      object.m_int = value;
    }

    inline void ToSerializationObject(SerializationObject& object, const uint64_t& value)
    {
      object.Type = SerializationVariableType::Uint;
      object.m_uint = value;
    }

    inline void ToSerializationObject(SerializationObject& object, const int32_t& value)
    {
      return ToSerializationObject(object, int64_t(value));
    }

    inline void ToSerializationObject(SerializationObject& object, const uint32_t& value)
    {
      return ToSerializationObject(object, uint64_t(value));
    }

    inline void ToSerializationObject(SerializationObject& object, const bool& value)
    {
      object.Type = SerializationVariableType::Bool;
      object.m_int = value;
    }

    inline void ToSerializationObject(SerializationObject& object, const std::string& value)
    {
      object.Type = SerializationVariableType::String;
      object.m_string = value;
    }

    void ToSerializationObject(SerializationObject& object, const TaskSharedStatus& value);

    inline void ToSerializationObject(SerializationObject& object, const SerializationObject& other)
    {
      object.Type = other.Type;
      AZURE_ASSERT(object.Context == other.Context);
      object.m_int = other.m_int;
      object.m_uint = other.m_uint;
      object.m_string = other.m_string;
      object.m_array = other.m_array;
      object.m_dict = other.m_dict;
    }

    template <class T>
    typename std::enable_if_t<_detail::is_shared_ptr_v<T>, void> ToSerializationObject(
        SerializationObject& object,
        const T& value)
    {
      void* rawPtr = value.get();
      auto ite = object.Context->m_sharedPointerMap.find(rawPtr);
      if (ite != object.Context->m_sharedPointerMap.end())
      {
        size_t index = ite->second;
        return ToSerializationObject(object, index);
      }
      size_t index = object.Context->m_sharedPointerMap.size();
      object.Context->m_sharedPointerMap[rawPtr] = index;
      auto obj = object.GetObject();
      ToSerializationObject(obj, *value);
      object.Context->SharedObjects.PushBack(std::move(obj));
      return ToSerializationObject(object, value);
    }

    template <class T>
    typename std::enable_if_t<_detail::is_vector_v<T>, void> ToSerializationObject(
        SerializationObject& object,
        const T& value)
    {
      object.Type = SerializationVariableType::Array;
      for (const auto& v : value)
      {
        auto obj = object.GetObject();
        ToSerializationObject(obj, v);
        object.PushBack(std::move(obj));
      }
    }

    template <class T>
    typename std::enable_if_t<std::is_enum<T>::value, void> ToSerializationObject(
        SerializationObject& object,
        const T& value)
    {
      return ToSerializationObject(object, static_cast<std::underlying_type_t<T>>(value));
    }

    inline void FromSerializationObject(const SerializationObject& object, int64_t& value)
    {
      value = object.m_int;
    }

    inline void FromSerializationObject(const SerializationObject& object, uint64_t& value)
    {
      value = object.m_uint;
    }

    inline void FromSerializationObject(const SerializationObject& object, int32_t& value)
    {
      int64_t value2;
      FromSerializationObject(object, value2);
      value = static_cast<int32_t>(value2);
    }

    inline void FromSerializationObject(const SerializationObject& object, uint32_t& value)
    {
      uint64_t value2;
      FromSerializationObject(object, value2);
      value = static_cast<uint32_t>(value2);
    }

    inline void FromSerializationObject(const SerializationObject& object, bool& value)
    {
      int64_t value2;
      FromSerializationObject(object, value2);
      value = static_cast<bool>(value2);
    }

    inline void FromSerializationObject(const SerializationObject& object, std::string& value)
    {
      value = object.m_string;
    }

    void FromSerializationObject(const SerializationObject& object, TaskSharedStatus& value);

    template <class T>
    typename std::enable_if_t<_detail::is_shared_ptr_v<T>, void> FromSerializationObject(
        const SerializationObject& object,
        T& value)
    {
      size_t index = object.Get<size_t>();
      if (index < object.Context->m_sharedPointerVector.size())
      {
        T value2 = std::static_pointer_cast<T::element_type>(
            object.Context->m_sharedPointerVector[index].lock());
        if (value2)
        {
          value = std::move(value2);
          return;
        }
      }
      if (!value)
      {
        value = std::make_shared<T::element_type>();
      }
      FromSerializationObject(object.Context->SharedObjects[index], *value);
      if (index >= object.Context->m_sharedPointerVector.size())
      {
        object.Context->m_sharedPointerVector.resize(index + 1);
      }
      object.Context->m_sharedPointerVector[index] = value;
    }

    template <class T>
    typename std::enable_if_t<std::is_enum<T>::value, void> FromSerializationObject(
        const SerializationObject& object,
        T& value)
    {
      std::underlying_type_t<T> value2;
      FromSerializationObject(object, value2);
      value = static_cast<T>(value2);
    }

  } // namespace _internal
}} // namespace Azure::Storage
