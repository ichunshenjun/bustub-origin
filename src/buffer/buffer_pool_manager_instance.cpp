//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t fid;
  if (!free_list_.empty()) {
    fid = free_list_.front();
    free_list_.pop_front();
  } else if (replacer_->Evict(&fid)) {
    if (pages_[fid].IsDirty()) {
      disk_manager_->WritePage(pages_[fid].GetPageId(), pages_[fid].GetData());
    }
    page_table_->Remove(pages_[fid].GetPageId());
  } else {
    return nullptr;
  }
  *page_id = AllocatePage();
  pages_[fid].page_id_ = *page_id;
  pages_[fid].is_dirty_ = false;
  pages_[fid].pin_count_ = 1;
  pages_[fid].ResetMemory();
  replacer_->RecordAccess(fid);
  replacer_->SetEvictable(fid, false);
  page_table_->Insert(*page_id, fid);
  return &pages_[fid];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t fid;
  if (page_table_->Find(page_id, fid)) {
    replacer_->RecordAccess(fid);
    replacer_->SetEvictable(fid, false);
    pages_[fid].pin_count_++;
    return &pages_[fid];
  }
  if (!free_list_.empty()) {
    fid = free_list_.front();
    free_list_.pop_front();
    pages_[fid].is_dirty_ = false;
    pages_[fid].pin_count_ = 1;
    pages_[fid].page_id_ = page_id;
  } else if (replacer_->Evict(&fid)) {
    if (pages_[fid].IsDirty()) {
      disk_manager_->WritePage(pages_[fid].GetPageId(), pages_[fid].GetData());
    }
    page_table_->Remove(pages_[fid].GetPageId());
  } else {
    return nullptr;
  }
  pages_[fid].pin_count_ = 1;
  pages_[fid].page_id_ = page_id;
  pages_[fid].is_dirty_ = false;
  pages_[fid].ResetMemory();
  disk_manager_->ReadPage(page_id, pages_[fid].data_);
  replacer_->RecordAccess(fid);
  replacer_->SetEvictable(fid, false);
  page_table_->Insert(page_id, fid);
  return &pages_[fid];
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t fid;
  if (page_table_->Find(page_id, fid)) {
    if (pages_[fid].pin_count_ == 0) {
      return false;
    }
    pages_[fid].pin_count_--;
    if (is_dirty) {
      pages_[fid].is_dirty_ = is_dirty;
    }
    if (pages_[fid].pin_count_ == 0) {
      replacer_->SetEvictable(fid, true);
    }
    return true;
  }
  return false;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  frame_id_t fid;
  if (page_table_->Find(page_id, fid)) {
    disk_manager_->WritePage(pages_[fid].GetPageId(), pages_[fid].GetData());
    return true;
  }
  return false;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock<std::mutex> lock(latch_);
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].page_id_ != INVALID_PAGE_ID) {
      disk_manager_->WritePage(pages_[i].GetPageId(), pages_[i].GetData());
    }
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t fid;
  if (page_table_->Find(page_id, fid)) {
    if (pages_[fid].pin_count_ > 0) {
      return false;
    }
    replacer_->Remove(fid);
    page_table_->Remove(page_id);
    pages_[fid].ResetMemory();
    pages_[fid].page_id_ = INVALID_PAGE_ID;
    pages_[fid].is_dirty_ = false;
    pages_[fid].pin_count_ = 0;
    free_list_.push_back(fid);
    return true;
  }
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
