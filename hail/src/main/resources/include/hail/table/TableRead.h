#ifndef HAIL_TABLEREAD_H
#define HAIL_TABLEREAD_H 1

#include "hail/table/PartitionContext.h"
#include "hail/Region.h"

namespace hail {

template<typename Consumer, typename Decoder>
class TableNativeRead {
  private:
    Consumer next_;
    Decoder dec_;
  public:
    using Endpoint = typename Consumer::Endpoint;
    Endpoint * end() { return next_.end(); }
    PartitionContext * ctx() { return next_.ctx(); }

    void consume() {
      auto region = ctx()->pool_.get_region();
      next_(std::move(region), dec_.decode_row(region.get()));
    }

    bool advance() {
      return dec_.decode_byte();
    }

    template<typename ... Args>
    explicit TableNativeRead(Decoder &&dec, Args ... args) :
    next_(args...),
    dec_(std::move(dec)) { }
    TableNativeRead() = delete;
    TableNativeRead(TableNativeRead &r) = delete;
    TableNativeRead(TableNativeRead &&r) = delete;
    TableNativeRead &operator=(TableNativeRead &r) = delete;
    TableNativeRead &operator=(TableNativeRead &&r) = delete;
    ~TableNativeRead() = default;
};

}

#endif
