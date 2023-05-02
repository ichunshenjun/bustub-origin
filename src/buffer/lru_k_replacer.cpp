//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (!fifo_.empty()) {
    for (auto iter = fifo_.begin(); iter != fifo_.end(); iter++) {
      if (frame_info_[*iter].set_evictable_) {
        *frame_id = *iter;
        fifo_.erase(iter);
        frame_info_.erase(*frame_id);
        curr_size_--;
        // LOG_DEBUG("fifo Evict %d", *frame_id);
        return true;
      }
    }
  }
  if (!lru_.empty()) {
    for (auto iter = lru_.begin(); iter != lru_.end(); iter++) {
      if (frame_info_[*iter].set_evictable_) {
        *frame_id = *iter;
        lru_.erase(iter);
        frame_info_.erase(*frame_id);
        curr_size_--;
        // LOG_DEBUG("lru Evict %d", *frame_id);
        return true;
      }
    }
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  auto iter_map = frame_info_.find(frame_id);
  if (iter_map == frame_info_.end()) {
    FrameEntry temp;
    temp.set_evictable_ = true;
    auto insert_res = frame_info_.insert(std::make_pair(frame_id, temp));
    iter_map = insert_res.first;
    curr_size_++;
  }
  iter_map->second.hit_count_++;
  if (iter_map->second.hit_count_ < k_) {
    auto iter_fifo = std::find(fifo_.begin(), fifo_.end(), frame_id);
    if (iter_fifo == fifo_.end()) {
      fifo_.push_back(frame_id);
    }
  } else if (iter_map->second.hit_count_ == k_) {
    fifo_.remove(frame_id);
    lru_.push_back(frame_id);
  } else {
    iter_map->second.hit_count_ = k_;
    auto iter_lru = std::find(lru_.begin(), lru_.end(), frame_id);
    if (iter_lru != fifo_.end()) {
      lru_.erase(iter_lru);
    }
    lru_.push_back(frame_id);
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frame_info_.find(frame_id) != frame_info_.end()) {
    if (frame_info_[frame_id].set_evictable_ && !set_evictable) {
      curr_size_--;
    } else if (!frame_info_[frame_id].set_evictable_ && set_evictable) {
      curr_size_++;
    }
    frame_info_[frame_id].set_evictable_ = set_evictable;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  auto iter_map = frame_info_.find(frame_id);
  if (iter_map == frame_info_.end()) {
    return;
  }
  if (iter_map->second.set_evictable_) {
    if (iter_map->second.hit_count_ < k_) {
      fifo_.remove(frame_id);
    } else {
      lru_.remove(frame_id);
    }
    frame_info_.erase(frame_id);
    curr_size_--;
  }
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  // size_t num_frames = 0;
  // for (auto iter : frame_info_) {
  //   if (iter.second.set_evictable_) {
  //     num_frames++;
  //   }
  // }
  // replacer_size_ = num_frames;
  // return num_frames;
  return curr_size_;
}

}  // namespace bustub
