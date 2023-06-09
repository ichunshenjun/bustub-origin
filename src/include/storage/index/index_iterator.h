//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator();
  explicit IndexIterator(BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *leaf_node, int pos);
  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool { return leaf_node_ == itr.leaf_node_ && pos_ == itr.pos_; }

  auto operator!=(const IndexIterator &itr) const -> bool { return leaf_node_ != itr.leaf_node_ || pos_ != itr.pos_; }

 private:
  // add your own private member variables here
  BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *leaf_node_;
  int pos_;
  BufferPoolManager *buffer_pool_manager_;
};

}  // namespace bustub
