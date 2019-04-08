#pragma once

#include "mongo/platform/basic.h"

#include <boost/intrusive_ptr.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/tag.hpp>
#include <boost/multi_index/sequenced_index.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index_container.hpp>
#include <boost/optional.hpp>
#include <vector>

#include "mongo/base/string_data_comparator_interface.h"
#include "mongo/db/exec/plan_stage.h"
#include "mongo/db/record_id.h"
#include "mongo/stdx/functional.h"

namespace mongo {

using boost::multi_index_container;
using boost::multi_index::sequenced;
using boost::multi_index::hashed_unique;
using boost::multi_index::member;
using boost::multi_index::tag;
using boost::multi_index::indexed_by;
using boost::multi_index::ordered_non_unique;


class TextMapIndex {

public:
    struct IndexData{
      IndexData(): wsid(WorkingSet::INVALID_ID), score(0.0), advanced(false), collected(false) {}
      RecordId recordId;
      WorkingSetID wsid;
      double score;
      bool advanced;
      std::vector<double> scoreTerms;
      bool collected;

    };
    struct Records {};
    struct Score {};

    using IndexContainer = 
      multi_index_container<
      IndexData,
      indexed_by< // list of indexes
          hashed_unique<  //hashed index over 'l'
            tag<Records>, // give that index a name
            member<IndexData, RecordId, &IndexData::recordId>, // what will be the index's key
            RecordId::Hasher
          >,
          ordered_non_unique<  //ordered index over 'i1'
            tag<Score>, // give that index a name
            member<IndexData, double, &IndexData::score> // what will be the index's key
          >
      >
    >;

    IndexData find(const RecordId& recordId) {
      auto it = boost::multi_index::get<Records>(_container).find(recordId);
      if (it != boost::multi_index::get<Records>(_container).end()) {
        return *it;
      }
      return IndexData();
    }

    void insert(IndexData data) {
      _container.insert(data);
    }

    /**
     * Returns the number of elements in the cache.
     */
    size_t size() const {
        return _container.size();
    }


private:
    IndexContainer _container;
};
}