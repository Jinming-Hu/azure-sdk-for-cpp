// Copyright (c) Microsoft Corporation. All rights reserved.
// SPDX-License-Identifier: MIT

#pragma once

#include <cstdint>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <thread>
#include <type_traits>
#include <vector>

namespace Azure { namespace Storage {
  namespace _detail {

    using JournalKey = std::vector<std::string>;

    enum class JournalNodeType
    {
      Null,
      Int,
      String,
    };

    struct JournalNode final
    {
      JournalNode() = default;
      JournalNode(const JournalNode&) = delete;
      JournalNode(JournalNode&&) = default;
      JournalNode& operator=(const JournalNode&) = delete;
      JournalNode& operator=(JournalNode&&) = default;

      JournalNodeType m_type = JournalNodeType::Null;

      // TODO: use union
      // items <= key have finished, except those in children
      std::string m_stringKey;
      int64_t m_intKey{0};

      bool m_done{false};

      std::string m_name;
      std::map<std::string, std::unique_ptr<JournalNode>> m_children;
      JournalNode* m_parent = nullptr;
    };

    struct JournalOperation final
    {
      enum class OperationType
      {
        Create,
        SetValue,
        SetDone,
        Snapshot,
      };
      OperationType Type = static_cast<OperationType>(0);
      JournalKey Operand;
      JournalNodeType Operand2Type = static_cast<JournalNodeType>(0);
      // TODO: use union
      std::string Operand2s;
      int64_t Operand2i{0};
      std::promise<std::string> Operand2f;
    };

  } // namespace _detail

  namespace _internal {
    class JournalTree final {
    public:
      JournalTree();
      JournalTree(const JournalTree&) = delete;
      JournalTree& operator=(const JournalTree&) = delete;
      ~JournalTree();

      std::string ToString();

      void AddOperation(_detail::JournalOperation op);

    private:
      void ProcessOperations(std::vector<_detail::JournalOperation>& ops);

      std::unique_ptr<_detail::JournalNode> m_root;

      std::vector<_detail::JournalOperation> m_operationQueue;
      std::mutex m_operationQueueMutex;
      std::condition_variable m_operationQueueCond;
      std::thread m_operationExecutor;
      std::atomic<bool> m_executorStop{false};

      friend class JournalAgent;
    };

    class JournalAgent final {
    public:
      JournalAgent() = default;
      explicit JournalAgent(JournalTree& tree) : m_tree(&tree) {}
      JournalAgent(const JournalAgent&) = delete;
      JournalAgent(JournalAgent&& other) noexcept { *this = std::move(other); }
      JournalAgent& operator=(const JournalAgent&) = delete;
      JournalAgent& operator=(JournalAgent&& other) noexcept;
      ~JournalAgent();
      explicit operator bool() const noexcept { return m_tree != nullptr; }

      JournalAgent CreateChild(const std::string& name) const;
      void SetValue(std::string value) const;
      void SetValue(int64_t value) const;
      void SetDone() const;

    private:
      JournalTree* m_tree = nullptr;
      _detail::JournalKey m_key;
    };

  } // namespace _internal
}} // namespace Azure::Storage
