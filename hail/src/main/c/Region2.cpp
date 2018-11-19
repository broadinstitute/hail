#include "hail/Region2.h"
#include "hail/Upcalls.h"
#include <memory>
#include <vector>
#include <utility>
#include "catch.hpp"

namespace hail {

RegionPool::Region::Region(RegionPool * pool) :
pool_(pool),
block_offset_(0),
current_block_(pool_->get_block()),
used_blocks_(std::vector<char *>()),
big_chunks_(std::vector<char *>()),
parents_(std::vector<std::shared_ptr<Region2>>()) { }

char * Region2::allocate_new_block() {
  used_blocks_.push_back(current_block_);
  current_block_ = pool_->get_block();
  block_offset_ = 0;
  return current_block_;
}

char * Region2::allocate_big_chunk(ssize_t n) {
  char * ptr = (char *) malloc(n);
  big_chunks_.push_back(ptr);
  return ptr;
}

void Region2::clear() {
  block_offset_ = 0;
  for (char * block : used_blocks_) { pool_->free_blocks_.push_back(block); }
  for (char * chunk : big_chunks_) { free(chunk); }
  used_blocks_.clear();
  big_chunks_.clear();
  parents_.clear();
}

std::shared_ptr<Region2> Region2::get_region() {
  return pool_->get_region();
}

void Region2::add_reference_to(std::shared_ptr<Region2> region) {
  parents_.push_back(region);
}

void RegionPool::RegionDeleter::operator()(Region2* p) const {
  p->clear();
  pool_->free_regions_.push_back(std::move(p));
}

RegionPool::RegionPool() :
free_regions_(std::vector<Region2 *>()),
free_blocks_(std::vector<char *>()),
del_(RegionDeleter(this)) { }

RegionPool::~RegionPool() {
  for (Region2 * region : free_regions_) {
    delete region;
  }
  for (char * block : free_blocks_) {
    free(block);
  }
}

char * RegionPool::get_block() {
  if (free_blocks_.empty()) {
    return (char *) malloc(block_size);
  }
  char * block = free_blocks_.back();
  free_blocks_.pop_back();
  return block;
}

std::shared_ptr<Region2> RegionPool::new_region() {
  return std::shared_ptr<Region2>(new Region2(this), del_);
}

std::shared_ptr<Region2> RegionPool::get_region() {
  if (free_regions_.empty()) {
    return new_region();
  }
  Region2 * region = std::move(free_regions_.back());
  free_regions_.pop_back();
  return std::shared_ptr<Region2>(region, del_);
}

TEST_CASE("region pools allocate and manage regions/blocks") {
  RegionPool pool;
  REQUIRE(pool.num_free_regions() == 0);
  REQUIRE(pool.num_free_blocks() == 0);

  SECTION("regions can be requested from pool") {
    auto region = pool.get_region();

    SECTION("freeing requested region returns to pool") {
      region = nullptr;
      REQUIRE(pool.num_free_regions() == 1);
      REQUIRE(pool.num_free_blocks() == 0);
    }

    SECTION("blocks can be acquired for region") {
      region->allocate(4, 64*1024 - 3);

      REQUIRE(pool.num_free_regions() == 0);
      REQUIRE(pool.num_free_blocks() == 0);

      SECTION("blocks are not released until region is released") {
        region->allocate(4, 10);
        REQUIRE(pool.num_free_blocks() == 0);
        region = nullptr;
        REQUIRE(pool.num_free_regions() == 1);
        REQUIRE(pool.num_free_blocks() == 1);
      }

      SECTION("large chunks are not returned to block pool") {
        region->allocate(4, 5000);
        REQUIRE(pool.num_free_blocks() == 0);
        region = nullptr;
        REQUIRE(pool.num_free_regions() == 1);
        REQUIRE(pool.num_free_blocks() == 0);
      }
    }

    SECTION("referenced regions are not freed until referencing region is freed") {
      auto region2 = region->get_region();
      region2->add_reference_to(region);
      region = nullptr;
      REQUIRE(pool.num_free_regions() == 0);
      region2 = nullptr;
      REQUIRE(pool.num_free_regions() == 2);
    }
  }
}

}