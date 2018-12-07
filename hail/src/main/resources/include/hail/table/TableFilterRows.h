#ifndef HAIL_TABLEFILTERROWS_H
#define HAIL_TABLEFILTERROWS_H 1

#include "hail/table/PartitionContext.h"
#include "hail/Region.h"

namespace hail {

template<typename Consumer, typename Filter>
class TableFilterRows {
  private:
    Consumer next_;
    Filter filter_{};

  public:
    using Endpoint = typename Consumer::Endpoint;
    Endpoint * end() { return next_.end(); }
    PartitionContext * ctx() { return next_.ctx(); }

    void operator()(RegionPtr &&region, const char * value) {
      if (filter_(region.get(), ctx()->globals_, value)) {
        next_(std::move(region), value);
      }
    }

    template<typename ... Args>
    explicit TableFilterRows(Args ... args) : next_(args...) { }
    TableFilterRows() = delete;
    TableFilterRows(TableFilterRows &r) = delete;
    TableFilterRows(TableFilterRows &&r) = delete;
    TableFilterRows &operator=(TableFilterRows &r) = delete;
    TableFilterRows &operator=(TableFilterRows &&r) = delete;
    ~TableFilterRows() = default;
};

}

#endif