
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

//#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/db/exec/text_or.h"

#include "mongo/db/exec/scoped_timer.h"
#include "mongo/db/exec/working_set_common.h"
#include "mongo/db/exec/working_set_computed_data.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/mongoutils/str.h"
//#include "mongo/util/log.h"

namespace mongo {

using std::unique_ptr;
using std::vector;
using stdx::make_unique;

// static
const char* TextOrStage::kStageType = "TEXT_OR";

TextOrStage::TextOrStage(OperationContext* opCtx,
                           WorkingSet* ws,
                           const FTSSpec& ftsSpec,
                           bool wantTextScore)
    : PlanStage(kStageType, opCtx),
      _ws(ws),
      _ftsSpec(ftsSpec),
      _currentChild(0),
      _indexerStatus(0),
      _wantTextScore(wantTextScore){
        _specificStats.wantTextScore = _wantTextScore;
      }

void TextOrStage::addChild(PlanStage* child) {
    _children.emplace_back(child);
    _specificStats.indexerCouter.push_back(0);
    _indexerStatus.push_back(0);
}

void TextOrStage::addChildren(Children childrenToAdd) {
    for (size_t i = 0; i < childrenToAdd.size(); ++i) {
        _specificStats.indexerCouter.push_back(0);
        _indexerStatus.push_back(0);
    }
    _children.insert(_children.end(),
                     std::make_move_iterator(childrenToAdd.begin()),
                     std::make_move_iterator(childrenToAdd.end()));
}

bool TextOrStage::isEOF() {
    return _internalState == State::kDone;
}

PlanStage::StageState TextOrStage::doWork(WorkingSetID* out) {
    if (isEOF()) {
        return PlanStage::IS_EOF;
    }

    PlanStage::StageState stageState = PlanStage::IS_EOF;
    // Optimization for one child to process
    if(1 == _children.size()) {
      _specificStats.singleChild = true;
      return readFromChild(out);
    }

    switch (_internalState) {
        case State::kReadingTerms:
            stageState = readFromChildren(out);
            break;
        case State::kReturningResults:
            stageState = returnResults(out);
            break;
        case State::kDone:
            // Should have been handled above.
            invariant(false);
            break;
    }

    return stageState;
}

double TextOrStage::getIndexScore(WorkingSetMember* member) {
  if (member->hasComputed(WSM_COMPUTED_TEXT_SCORE)) {
    const TextScoreComputedData* score = static_cast<const TextScoreComputedData*>(
                    member->getComputed(WSM_COMPUTED_TEXT_SCORE));
    return score->getScore();
  }
  const IndexKeyDatum newKeyData = member->keyData.back();
  
  BSONObjIterator keyIt(newKeyData.keyData);
  for (unsigned i = 0; i < _ftsSpec.numExtraBefore(); i++) {
      keyIt.next();
  }
  keyIt.next();  // Skip past 'term'.
  BSONElement scoreElement = keyIt.next();
  return scoreElement.number();
}

PlanStage::StageState TextOrStage::readFromChildren(WorkingSetID* out) {
    // Check to see if there were any children added in the first place.
    if (_children.size() == 0) {
        _internalState = State::kDone;
        return PlanStage::IS_EOF;
    }
    invariant(_currentChild < _children.size());

    WorkingSetID id = WorkingSet::INVALID_ID;
    StageState childStatus = _children[_currentChild]->work(&id);


    if (PlanStage::ADVANCED == childStatus) {
        WorkingSetMember* member = _ws->get(id);
        // Maybe the child had an invalidation.  We intersect RecordId(s) so we can't do anything
        // with this WSM.
        if (!member->hasRecordId()) {
            _ws->flagForReview(id);
            return PlanStage::NEED_TIME;
        }
        ++_specificStats.dupsTested;
        if(!_wantTextScore) {
          if (_dataMap.end() != _dataMap.find(member->recordId)) {
            ++_specificStats.dupsDropped;
            _ws->free(id);
            return PlanStage::NEED_TIME;
          } else {
            TextRecordData textRecordData;
            textRecordData.wsid = id;
            if (!_dataMap.insert(std::make_pair(member->recordId, textRecordData)).second) {
              // Didn't insert because we already had this RecordId inside the map. This should only
              // happen if we're seeing a newer copy of the same doc in a more recent snapshot.
              // Throw out the newer copy of the doc.
              _ws->free(id);
              return PlanStage::NEED_TIME;
            }
            *out = id;
            return PlanStage::ADVANCED;
          }
        }
        double documentTermScore = getIndexScore(member);
        DataMap::iterator it = _dataMap.find(member->recordId);
        // Found. Store extra.
        if (_dataMap.end() != it) {
            it->second.score += documentTermScore;
            ++_specificStats.dupsDropped;
            _ws->free(id);
            return PlanStage::NEED_TIME;
        }

        TextRecordData textRecordData;
        textRecordData.score = documentTermScore;
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
    } else if (PlanStage::IS_EOF == childStatus) {

        // Done with _currentChild, move to the next one.
        ++_currentChild;

        if (_currentChild < _children.size()) {
            // We have another child to read from.
            return PlanStage::NEED_TIME;
        }

        if(!_wantTextScore) {
          return PlanStage::IS_EOF;  
        }

        _scoreIterator = _dataMap.begin();
        _internalState = State::kReturningResults;

        return PlanStage::NEED_TIME; 
    }

    // NEED_TIME, ERROR, NEED_YIELD, pass them up.
    *out = id;
    return childStatus;
}

PlanStage::StageState TextOrStage::readFromChild(WorkingSetID* out) {
    WorkingSetID id = WorkingSet::INVALID_ID;
    StageState childStatus = _children[_currentChild]->work(&id);

    if (PlanStage::ADVANCED == childStatus) {
        WorkingSetMember* member = _ws->get(id);
        // Maybe the child had an invalidation.  We intersect RecordId(s) so we can't do anything
        // with this WSM.
        if (!member->hasRecordId()) {
            _ws->flagForReview(id);
            return PlanStage::NEED_TIME;
        }
        
        if(!_wantTextScore) {
          *out = id;
          return PlanStage::ADVANCED;
        }
        
        TextRecordData textRecordData;
        textRecordData.score = getIndexScore(member);
        textRecordData.wsid = id;
        if (member->hasComputed(WSM_COMPUTED_TEXT_SCORE)) {
          member->updateComputed(new TextScoreComputedData(textRecordData.score));
        } else {
          member->addComputed(new TextScoreComputedData(textRecordData.score));
        }
    }

    // NEED_TIME, ERROR, NEED_YIELD, pass them up.
    *out = id;
    return childStatus;
}

PlanStage::StageState TextOrStage::returnResults(WorkingSetID* out) {
    //LOG(3) << "stage returnResults";
    if (_scoreIterator == _dataMap.end()) {
        _internalState = State::kDone;
        return PlanStage::IS_EOF;
    }

    // Retrieve the record that contains the text score.
    TextRecordData textRecordData = _scoreIterator->second;
    ++_scoreIterator;
      //LOG(3) << "stage returnResults " << textRecordData.wsid << " score" << textRecordData.score;
    // Ignore non-matched documents.
    if (textRecordData.score < 0) {
        invariant(textRecordData.wsid == WorkingSet::INVALID_ID);
        return PlanStage::NEED_TIME;
    }

    WorkingSetMember* wsm = _ws->get(textRecordData.wsid);
    // Populate the working set member with the text score and return it.
    if (wsm->hasComputed(WSM_COMPUTED_TEXT_SCORE)) {
      wsm->updateComputed(new TextScoreComputedData(textRecordData.score));
    } else {
      wsm->addComputed(new TextScoreComputedData(textRecordData.score));
    }
    *out = textRecordData.wsid;
    return PlanStage::ADVANCED;
}

void TextOrStage::doInvalidate(OperationContext* opCtx, const RecordId& dl, InvalidationType type) {
    // TODO remove this since calling isEOF is illegal inside of doInvalidate().
    if (isEOF()) {
        return;
    }

    DataMap::iterator it = _dataMap.find(dl);
    if (_dataMap.end() != it) {
        WorkingSetID id = it->second.wsid;
        WorkingSetMember* member = _ws->get(id);
        verify(member->recordId == dl);

        // Add the WSID to the to-be-reviewed list in the WS.
        _ws->flagForReview(id);
        ++_specificStats.recordIdsForgotten;
        // And don't return it from this stage.
        _dataMap.erase(it);
    }
}

unique_ptr<PlanStageStats> TextOrStage::getStats() {
    _commonStats.isEOF = isEOF();

    unique_ptr<PlanStageStats> ret = make_unique<PlanStageStats>(_commonStats, STAGE_TEXT_OR);
    ret->specific = make_unique<TextOrStats>(_specificStats);
    for (size_t i = 0; i < _children.size(); ++i) {
        ret->children.emplace_back(_children[i]->getStats());
    }

    return ret;
}

const SpecificStats* TextOrStage::getSpecificStats() const {
    return &_specificStats;
}

}  // namespace mongo
