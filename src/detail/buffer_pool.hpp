#pragma once

#include <cstddef>
#include <memory>
#include <mutex>
#include <span>
#include <vector>

namespace uvnet::detail {

// 固定大小缓冲块的引用计数池。
// acquire 返回的 keepalive 释放后，块会自动回到空闲池（线程安全）；
// 若调用方把 keepalive 传给零拷贝写，块会一直保留到写完成。
class BufferPool : public std::enable_shared_from_this<BufferPool> {
 public:
  static std::shared_ptr<BufferPool> create(std::size_t block_size,
                                            std::size_t max_free_blocks) {
    return std::shared_ptr<BufferPool>(new BufferPool(block_size, max_free_blocks));
  }

  struct Handle {
    std::span<std::byte> data;
    std::shared_ptr<const void> keepalive;
  };

  ~BufferPool() {
    for (auto* block : free_blocks_) {
      delete block;
    }
  }

  Handle acquire() {
    Block* block = nullptr;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (!free_blocks_.empty()) {
        block = free_blocks_.back();
        free_blocks_.pop_back();
      }
    }

    if (block == nullptr) {
      block = new Block();
      block->bytes.resize(block_size_);
    }

    std::shared_ptr<Block> owner(block, [pool = shared_from_this()](Block* released) {
      pool->recycle(released);
    });
    Handle handle;
    handle.data = std::span<std::byte>(block->bytes.data(), block->bytes.size());
    handle.keepalive = owner;
    return handle;
  }

 private:
  struct Block {
    std::vector<std::byte> bytes;
  };

  BufferPool(std::size_t block_size, std::size_t max_free_blocks)
      : block_size_(block_size), max_free_blocks_(max_free_blocks) {}

  void recycle(Block* block) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (free_blocks_.size() >= max_free_blocks_) {
      delete block;
      return;
    }
    free_blocks_.push_back(block);
  }

  std::size_t block_size_;
  std::size_t max_free_blocks_;
  std::mutex mutex_;
  std::vector<Block*> free_blocks_;
};

} // namespace uvnet::detail
