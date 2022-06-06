// Copyright (c) Microsoft Corporation. All rights reserved.
// SPDX-License-Identifier: MIT

#include <azure/storage/datamovement/journal.hpp>

#include <functional>

#include <azure/core/azure_assert.hpp>
#include <azure/core/internal/json/json.hpp>

namespace Azure { namespace Storage { namespace _internal {

  std::string JournalTree::ToString()
  {
    _detail::JournalOperation op;
    op.Type = _detail::JournalOperation::OperationType::Snapshot;
    std::future<std::string> s = op.Operand2f.get_future();
    AddOperation(std::move(op));
    return s.get();
  }

  JournalTree::JournalTree() : m_root(std::make_unique<_detail::JournalNode>())
  {
    m_operationExecutor = std::thread([this]() {
      std::unique_lock<std::mutex> guard(m_operationQueueMutex, std::defer_lock);
      while (true)
      {
        guard.lock();
        m_operationQueueCond.wait_for(guard, std::chrono::seconds(1), [this]() {
          return !m_operationQueue.empty() || m_executorStop.load(std::memory_order_relaxed);
        });

        std::vector<_detail::JournalOperation> ops;
        ops.swap(m_operationQueue);
        guard.unlock();
        ProcessOperations(ops);

        if (m_executorStop.load(std::memory_order_relaxed))
        {
          break;
        }
      }
    });
  }

  void JournalTree::ProcessOperations(std::vector<_detail::JournalOperation>& ops)
  {
    for (auto& op : ops)
    {
      _detail::JournalNode* operand = m_root.get();
      for (const auto& k : op.Operand)
      {
        operand = operand->m_children[k].get();
      }
      if (op.Type == _detail::JournalOperation::OperationType::Create)
      {
        auto ite = operand->m_children.find(op.Operand2s);
        if (ite == operand->m_children.end())
        {
          auto newNode = std::make_unique<_detail::JournalNode>();
          newNode->m_name = op.Operand2s;
          newNode->m_parent = operand;
          ite = operand->m_children.emplace(op.Operand2s, std::move(newNode)).first;
        }
      }
      else if (op.Type == _detail::JournalOperation::OperationType::SetValue)
      {
        if (operand->m_type == _detail::JournalNodeType::Null)
        {
          operand->m_type = op.Operand2Type;
        }
        else
        {
          AZURE_ASSERT(operand->m_type == op.Operand2Type);
        }
        if (op.Operand2Type == _detail::JournalNodeType::Int)
        {
          operand->m_intKey = op.Operand2i;
        }
        else if (op.Operand2Type == _detail::JournalNodeType::String)
        {
          operand->m_stringKey = op.Operand2s;
        }
      }
      else if (op.Type == _detail::JournalOperation::OperationType::SetDone)
      {
        operand->m_done = true;
        while (operand)
        {
          if (!operand->m_children.empty() || !operand->m_done)
          {
            break;
          }
          _detail::JournalNode* parent = operand->m_parent;
          if (!parent)
          {
            break;
          }
          const auto operandName = operand->m_name;
          parent->m_children.erase(operandName);
          operand = parent;
        }
      }
      else if (op.Type == _detail::JournalOperation::OperationType::Snapshot)
      {
        std::function<Core::Json::_internal::json(_detail::JournalNode*)> serializeNode;
        serializeNode = [&serializeNode](_detail::JournalNode* node) {
          Core::Json::_internal::json serializedObject;
          serializedObject["name"] = node->m_name;
          serializedObject["type"]
              = static_cast<std::underlying_type_t<_detail::JournalNodeType>>(node->m_type);
          serializedObject["done"] = node->m_done;
          if (!node->m_done)
          {
            switch (node->m_type)
            {
              case _detail::JournalNodeType::Int:
                serializedObject["key"] = node->m_intKey;
                break;
              case _detail::JournalNodeType::String:
                serializedObject["key"] = node->m_stringKey;
                break;
            }
          }
          serializedObject["children"] = Core::Json::_internal::json::array();
          for (auto& p : node->m_children)
          {
            serializedObject["children"].push_back(serializeNode(p.second.get()));
          }
          return serializedObject;
        };

        auto serializedObject = serializeNode(m_root.get());
        op.Operand2f.set_value(serializedObject.dump());
      }
    }
  }

  JournalTree::~JournalTree()
  {
    m_executorStop = true;
    m_operationQueueCond.notify_one();
    if (m_operationExecutor.joinable())
    {
      m_operationExecutor.join();
    }
    AZURE_ASSERT(m_root->m_done && m_root->m_children.empty());
  }

  void JournalTree::AddOperation(_detail::JournalOperation op)
  {
    {
      std::lock_guard<std::mutex> guard(m_operationQueueMutex);
      m_operationQueue.push_back(std::move(op));
    }
    m_operationQueueCond.notify_one();
  }

  JournalAgent& JournalAgent::operator=(JournalAgent&& other) noexcept
  {
    m_tree = other.m_tree;
    other.m_tree = nullptr;
    m_key = std::move(other.m_key);
    return *this;
  }

  JournalAgent::~JournalAgent()
  {
    if (m_tree)
    {
      SetDone();
    }
  }

  JournalAgent JournalAgent::CreateChild(const std::string& name) const
  {
    if (m_tree)
    {
      _detail::JournalOperation op;
      op.Type = _detail::JournalOperation::OperationType::Create;
      op.Operand = m_key;
      op.Operand2s = name;
      m_tree->AddOperation(std::move(op));
    }

    JournalAgent ret;
    ret.m_tree = m_tree;
    ret.m_key = m_key;
    ret.m_key.push_back(name);
    return ret;
  }

  void JournalAgent::SetValue(std::string value) const
  {
    if (!m_tree)
    {
      return;
    }
    _detail::JournalOperation op;
    op.Type = _detail::JournalOperation::OperationType::SetValue;
    op.Operand = m_key;
    op.Operand2Type = _detail::JournalNodeType::String;
    op.Operand2s = std::move(value);
    m_tree->AddOperation(std::move(op));
  }

  void JournalAgent::SetValue(int64_t value) const
  {
    if (!m_tree)
    {
      return;
    }
    _detail::JournalOperation op;
    op.Type = _detail::JournalOperation::OperationType::SetValue;
    op.Operand = m_key;
    op.Operand2Type = _detail::JournalNodeType::Int;
    op.Operand2i = value;
    m_tree->AddOperation(std::move(op));
  }

  void JournalAgent::SetDone() const
  {
    if (!m_tree)
    {
      return;
    }
    _detail::JournalOperation op;
    op.Type = _detail::JournalOperation::OperationType::SetDone;
    op.Operand = m_key;
    m_tree->AddOperation(std::move(op));
  }
}}} // namespace Azure::Storage::_internal
