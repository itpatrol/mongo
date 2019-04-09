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
            member<IndexData, double, &IndexData::score>,
            std::greater<double> // what will be the index's key
          >
      >
    >;

    typedef IndexContainer::index<Score>::type ScoreIndex;
    typedef IndexContainer::index<Records>::type RecordIndex;

    //TextMapIndex::const_iterator = IndexContainer::nth_index<1>::type::iterator;

   /* RecordIndex::iterator findByID(const RecordId& recordId) {
      auto it = boost::multi_index::get<Records>(_container).find(recordId);
      if (it != boost::multi_index::get<Records>(_container).end()) {
        return it;
      }
      return void;
    }*/

    RecordIndex::iterator findByID(const RecordId& recordId) {
      return boost::multi_index::get<Records>(_container).find(recordId);
    };

    ScoreIndex::iterator scoreIterator(){
      return _scoreIterator;
    };

    ScoreIndex::iterator endScore() {
      return boost::multi_index::get<Score>(_container).end();
    };

    bool isScoreEmpty() {
      if(boost::multi_index::get<Score>(_container).end() == boost::multi_index::get<Score>(_container).begin()) {
        return false;
      }
      return true;
    };
    
    RecordIndex::iterator endRecords() {
      return boost::multi_index::get<Records>(_container).end();
    };

    struct updateScore {
      updateScore(size_t termID, double newScore):termID(termID), newScore(newScore){}

      void operator()(IndexData& record)
      {
        record.score += newScore;
        record.scoreTerms[termID] = newScore;
      }

      private:
        size_t termID;
        double newScore;
    };

    struct trueAdvance {
      trueAdvance(bool advanced):advanced(advanced){}

      void operator()(IndexData& record)
      {
        record.advanced = advanced;
      }
      private:
        bool advanced;
    };

    void update(RecordIndex::iterator it, size_t termID, double newScore) {
      _container.modify(it, updateScore(termID, newScore));
    };

    void setAdvanced(const RecordId& recordId) {
      RecordIndex::iterator it = findByID(recordId);
      _container.modify(it, trueAdvance(true));
    };

    //bool updateScore()

    /*bool setByID(RecordId recordId, WorkingSetID wsid, double score, std::vector<double> scoreTerms) {
      auto it = boost::multi_index::get<Records>(_container).find(recordId);
      if (it != boost::multi_index::get<Records>(_container).end()) {
        IndexData currentRecord = *it;
      }
      IndexData newRecord;
      recordData.recordId = member->recordId;
      recordData.wsid = _currentWorkState.wsid;
      recordData.score = score;
      recordData.scoreTerms = scoreTerms;
      recordData.scoreTerms[_currentChild] = documentTermScore;
    }*/

    void resetScopeIterator() {
      _scoreIterator = boost::multi_index::get<Score>(_container).begin();
    }

    IndexData getScore(){
      if(_scoreIterator == boost::multi_index::get<Score>(_container).end()) {
        return IndexData();
      }
      return *_scoreIterator;
    }
    IndexData nextScore(){
      ++_scoreIterator;
      if(_scoreIterator == boost::multi_index::get<Score>(_container).end()) {
        return IndexData();
      }
      return *_scoreIterator;
    }

    /*auto findByScoreAll() {
      auto it = boost::multi_index::get<Score>(_container).begin();
      if (it != boost::multi_index::get<Score>(_container).end()) {
        return it;
      }
      return nullptr;
    }*/

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
    ScoreIndex::iterator _scoreIterator;
};
}
