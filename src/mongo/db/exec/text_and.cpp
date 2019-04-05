
/**
 *    Copyright (C) 2018-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */


#include "mongo/db/exec/text_and.h"
#include "mongo/db/exec/scoped_timer.h"
#include "mongo/db/exec/working_set_common.h"
#include "mongo/db/exec/working_set_computed_data.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {

using std::unique_ptr;
using std::vector;
using stdx::make_unique;

// static
const char* TextAndStage::kStageType = "TEXT_AND";

TextAndStage::TextAndStage(OperationContext* opCtx,
                           WorkingSet* ws,
                           const FTSSpec& ftsSpec,
                           bool wantTextScore,
                           Children childrenToAdd)
    : PlanStage(kStageType, opCtx),
      _ftsSpec(ftsSpec),
      _ws(ws),
      _intersectingChildren(true),
      _currentChild(0),
      _wantTextScore(wantTextScore){
        _specificStats.wantTextScore = _wantTextScore;
        for (size_t i = 0; i < childrenToAdd.size(); ++i) {
          _specificStats._counter.push_back(0);
        }
        _children.insert(_children.end(),
                        std::make_move_iterator(childrenToAdd.begin()),
                        std::make_move_iterator(childrenToAdd.end()));
      }
TextAndStage::TextAndStage(OperationContext* opCtx,
                           WorkingSet* ws,
                           const FTSSpec& ftsSpec,
                           bool wantTextScore)
    : PlanStage(kStageType, opCtx),
      _ftsSpec(ftsSpec),
      _ws(ws),
      _intersectingChildren(true),
      _currentChild(0),
      _wantTextScore(wantTextScore){
        _specificStats.wantTextScore = _wantTextScore;
      }
TextAndStage::~TextAndStage() {}

void TextAndStage::addChild(PlanStage* child) {
    _children.emplace_back(child);
    _specificStats._counter.push_back(0);
}

void TextAndStage::addChildren(Children childrenToAdd) {
    for (size_t i = 0; i < childrenToAdd.size(); ++i) {
      _specificStats._counter.push_back(0);
    }
    _children.insert(_children.end(),
                     std::make_move_iterator(childrenToAdd.begin()),
                     std::make_move_iterator(childrenToAdd.end()));
}

bool TextAndStage::isEOF() {
    // Either we're busy hashing children, in which case we're not done yet.
    if (_intersectingChildren) {
        return false;
    }

    // If there's nothing to probe against, we're EOF.
    if (_dataMap.empty()) {
        return true;
    }

    return _currentChild >= _children.size();
}

PlanStage::StageState TextAndStage::doWork(WorkingSetID* out) {
    if (isEOF()) {
        return PlanStage::IS_EOF;
    }

    

    WorkingSetID id = WorkingSet::INVALID_ID;
    StageState childStatus = _children[_currentChild]->work(&id);

    if (PlanStage::ADVANCED == childStatus) {
        
        WorkingSetMember* member = _ws->get(id);

        ++_specificStats._counter[_currentChild];
        // Maybe the child had an invalidation.  We intersect RecordId(s) so we can't do anything
        // with this WSM.
        if (!member->hasRecordId()) {
            _ws->flagForReview(id);
            return PlanStage::NEED_TIME;
        }

        // On second and other child - check for _dataMap
        if(0 < _currentChild) {
          ++_specificStats.dupsTested;
        
          DataMap::iterator it = _dataMap.find(member->recordId);
          // Not found, so dicard it as missing intersection.
          if (_dataMap.end() == it) {
            ++_specificStats.dupsDropped;
            _ws->free(id);
            return PlanStage::NEED_TIME;
          } else {
            if(_wantTextScore) {
                // compute score here.
                invariant(!member->keyData.empty());
                // copy to keep 
                const IndexKeyDatum newKeyData = member->keyData.back();
                
                BSONObjIterator keyIt(newKeyData.keyData);
                for (unsigned i = 0; i < _ftsSpec.numExtraBefore(); i++) {
                    keyIt.next();
                }
                keyIt.next();  // Skip past 'term'.
                BSONElement scoreElement = keyIt.next();
                double documentTermScore = scoreElement.number();
                it->second.score += documentTermScore;
            }
          }
        }
        if (_seenMap.end() != _seenMap.find(member->recordId)) {
            // .We already seen it. Drop.
            ++_specificStats.dupsDropped;
            _ws->free(id);
            return PlanStage::NEED_TIME;
        } else {
            // Otherwise, note that we've seen it.
            _seenMap.insert(member->recordId);
        }
        // We read the first child into our hash table.
        if(0 == _currentChild) {
          TextRecordData textRecordData;
          
          if(_wantTextScore) {
          // copy to keep 
              const IndexKeyDatum newKeyData = member->keyData.back();
              BSONObjIterator keyIt(newKeyData.keyData);
              for (unsigned i = 0; i < _ftsSpec.numExtraBefore(); i++) {
                  keyIt.next();
              }
              keyIt.next();  // Skip past 'term'.
              BSONElement scoreElement = keyIt.next();
              textRecordData.score = scoreElement.number();
          }
          textRecordData.wsid = id;
                
          if (!_dataMap.insert(std::make_pair(member->recordId, textRecordData)).second) {
            // Didn't insert because we already had this RecordId inside the map. This should only
            // happen if we're seeing a newer copy of the same doc in a more recent snapshot.
            // Throw out the newer copy of the doc.
            _ws->free(id);
            return PlanStage::NEED_TIME;
          }
          //member->makeObjOwnedIfNeeded();
          return PlanStage::NEED_TIME;
        }
        // It's last child - allow to ADVANCED
        if(_currentChild == _children.size() - 1) {
          if(_wantTextScore) {
            DataMap::iterator it = _dataMap.find(member->recordId);
            if (_dataMap.end() != it) {
              if (member->hasComputed(WSM_COMPUTED_TEXT_SCORE)) {
                member->updateComputed(new TextScoreComputedData(it->second.score));
              } else {
                member->addComputed(new TextScoreComputedData(it->second.score));
              }
            }
          }
          *out = id;
          return PlanStage::ADVANCED;
        }
        return PlanStage::NEED_TIME;
    } else if (PlanStage::IS_EOF == childStatus) {
        
        // Done with second or more child. Need to cleanup not found.
        if(0 < _currentChild) {
            DataMap::iterator it = _dataMap.begin();
            while (it != _dataMap.end()) {
                if (_seenMap.end() == _seenMap.find(it->first)) {
                    DataMap::iterator toErase = it;
                    ++it;

                    _ws->free(toErase->second.wsid);
                    _dataMap.erase(toErase);
                } else {
                    ++it;
                }
            }
        }
        _seenMap.clear();

        // If we have nothing to AND with after finishing any child, stop.
        if (_dataMap.empty()) {
            _intersectingChildren = false;
            return PlanStage::IS_EOF;
        }
        
        // Last child. Do cleanup
        if(_currentChild == _children.size() - 1) {
          _intersectingChildren = false;
          _dataMap.clear();
        }

        // Done with _currentChild, move to the next one.
        ++_currentChild;


        // Maybe we're out of children.
        if (isEOF()) {
            return PlanStage::IS_EOF;
        } else {
            return PlanStage::NEED_TIME;
        }
    } else if (PlanStage::FAILURE == childStatus || PlanStage::DEAD == childStatus) {
        // The stage which produces a failure is responsible for allocating a working set member
        // with error details.
        invariant(WorkingSet::INVALID_ID != id);
        *out = id;
        return childStatus;
    } else if (PlanStage::NEED_YIELD == childStatus) {
        *out = id;
    }

    // NEED_TIME, ERROR, NEED_YIELD, pass them up.
    return childStatus;
}

void TextAndStage::doInvalidate(OperationContext* opCtx, const RecordId& dl, InvalidationType type) {
    // TODO remove this since calling isEOF is illegal inside of doInvalidate().
    if (isEOF()) {
        return;
    }

    // If we see DL again it is not the same record as it once was so we still want to
    // return it.
    if (INVALIDATION_DELETION == type) {
        SeenMap::iterator it = _seenMap.find(dl);
        if (_seenMap.end() != it) {
            ++_specificStats.recordIdsForgotten;
            _seenMap.erase(dl);
        }
    }

    DataMap::iterator it = _dataMap.find(dl);
    if (_dataMap.end() != it) {
        WorkingSetID id = it->second.wsid;
        WorkingSetMember* member = _ws->get(id);
        verify(member->recordId == dl);

        // Add the WSID to the to-be-reviewed list in the WS.
        _ws->flagForReview(id);

        // And don't return it from this stage.
        _dataMap.erase(it);
    }
}

unique_ptr<PlanStageStats> TextAndStage::getStats() {
    _commonStats.isEOF = isEOF();

    unique_ptr<PlanStageStats> ret = make_unique<PlanStageStats>(_commonStats, STAGE_TEXT_AND);
    ret->specific = make_unique<TextAndStats>(_specificStats);
    for (size_t i = 0; i < _children.size(); ++i) {
        ret->children.emplace_back(_children[i]->getStats());
    }

    return ret;
}

const SpecificStats* TextAndStage::getSpecificStats() const {
    return &_specificStats;
}

}  // namespace mongo
