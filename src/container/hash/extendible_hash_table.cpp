//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <memory>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
      dir_.emplace_back(std::make_shared<Bucket>(bucket_size));
    }

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  // UNREACHABLE("not implemented");
  std::scoped_lock<std::mutex> lock(latch_);
  size_t dir_index=IndexOf(key);
  return dir_[dir_index]->Find(key,value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  // UNREACHABLE("not implemented");
  std::scoped_lock<std::mutex> lock(latch_);
  size_t dir_index=IndexOf(key);
  return dir_[dir_index]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  // UNREACHABLE("not implemented");
  std::scoped_lock<std::mutex> lock(latch_);
  size_t dir_index=IndexOf(key);
  bool success=dir_[dir_index]->Insert(key,value);
  while(!success){
    auto bucket=dir_[dir_index];
    auto local_depth=dir_[dir_index]->GetDepth();
    if(local_depth==global_depth_){
      global_depth_++;
      size_t size=dir_.size();
      for(size_t i=0;i<size;i++){
        dir_.emplace_back(dir_[i]);
      }
    }
    auto zero_bucket=std::make_shared<Bucket>(bucket_size_,local_depth+1);
    auto one_bucket=std::make_shared<Bucket>(bucket_size_,local_depth+1);
    for(auto [k,v]:bucket->GetItems()){
      auto bucket_index=std::hash<K>()(k) & ((1 << local_depth));
      if(bucket_index){
        one_bucket->Insert(k,v);
      }
      else{
        zero_bucket->Insert(k,v);
      }
    }
    // auto pos=dir_index&((1 << local_depth)); //这样写是错误的有可能有多个目录指向了同一个桶
    // if(pos){
    //   dir_[dir_index]=one_bucket;
    //   dir_[dir_index-dir_.size()/2]=zero_bucket;
    // }
    // else{
    //   dir_[dir_index]=zero_bucket;
    //   dir_[dir_index+dir_.size()/2]=one_bucket;
    // }
    for(size_t i=0;i<dir_.size();i++){
      if(dir_[i]==bucket){
        if(i&((1 << local_depth))){
          dir_[i]=one_bucket;
        }
        else{
          dir_[i]=zero_bucket;
        }
      }
    }
    num_buckets_++;
    dir_index=IndexOf(key);
    success=dir_[dir_index]->Insert(key,value);
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  // UNREACHABLE("not implemented");
  auto list=GetItems();
  for(auto [k,v]:list){
    if(key==k){
      value=v;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  // UNREACHABLE("not implemented");
  std::list<std::pair<K, V>> & list=GetItems();
  for(auto iter=list.begin();iter!=list.end();iter++){
    if(iter->first==key){
      list.erase(iter);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  // UNREACHABLE("not implemented");
  std::list<std::pair<K, V>> & list=GetItems();
  auto iter=list.begin();
  for(iter=list.begin();iter!=list.end();iter++){
    if(iter->first==key){
      break;
    }
  }
  if(iter!=list.end()){
    iter->second=value;
    return true;
  }
  if(!IsFull()){
    list.emplace_back(std::make_pair(key,value));
    return true;
  }
  return false;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
