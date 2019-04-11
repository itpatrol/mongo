
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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/db/exec/text_or.h"

#include "mongo/db/exec/scoped_timer.h"
#include "mongo/db/exec/working_set_common.h"
#include "mongo/db/exec/working_set_computed_data.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/log.h"

namespace mongo {

using std::unique_ptr;
using std::vector;
using stdx::make_unique;


/*
std::size_t hash_value(RecordId const& b){
    RecordId::Hasher hasher;
    return hasher(b);
};*/


// static
const char* TextOrStage::kStageType = "TEXT_OR";
const size_t TextOrStage::kChildIsEOF = -1;

TextOrStage::TextOrStage(OperationContext* opCtx,
                         WorkingSet* ws,
                         const FTSSpec& ftsSpec,
                         bool wantTextScore)
    : PlanStage(kStageType, opCtx),
      _ws(ws),
      _ftsSpec(ftsSpec),
      _currentChild(0),
      _indexerStatus(0),
      _scoreStatus(0),
      _wantTextScore(wantTextScore) {
    _specificStats.wantTextScore = _wantTextScore;
    //_tmiScoreIterator = _dataIndexMap.beginScore();
    _dataIndexMap.resetScopeIterator();
}

void TextOrStage::addChild(PlanStage* child) {
    _children.emplace_back(child);
    _specificStats.indexerCouter.push_back(0);
    _indexerStatus.push_back(0);
    _scoreStatus.push_back(0);
}

void TextOrStage::addChildren(Children childrenToAdd) {
    for (size_t i = 0; i < childrenToAdd.size(); ++i) {
        _specificStats.indexerCouter.push_back(0);
        _indexerStatus.push_back(0);
        _scoreStatus.push_back(0);
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
    if (1 == _children.size()) {
        _specificStats.singleChild = true;
        return readFromChild(out);
    }

    switch (_internalState) {
        case State::kReadingTerms:
            /*stageState = returnReadyResults(out);
            if(stageState != PlanStage::IS_EOF) {
                return stageState;
            }*/
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
        const TextScoreComputedData* score =
            static_cast<const TextScoreComputedData*>(member->getComputed(WSM_COMPUTED_TEXT_SCORE));
        currentAllTermsScore -= _scoreStatus[_currentChild];
        _scoreStatus[_currentChild] = score->getScore();
        currentAllTermsScore += _scoreStatus[_currentChild];
        return _scoreStatus[_currentChild];
    }
    const IndexKeyDatum newKeyData = member->keyData.back();

    BSONObjIterator keyIt(newKeyData.keyData);
    for (unsigned i = 0; i < _ftsSpec.numExtraBefore(); i++) {
        keyIt.next();
    }
    keyIt.next();  // Skip past 'term'.
    BSONElement scoreElement = keyIt.next();
    currentAllTermsScore -= _scoreStatus[_currentChild];
    _scoreStatus[_currentChild] = scoreElement.number();
    currentAllTermsScore += _scoreStatus[_currentChild];
    return _scoreStatus[_currentChild];
}

bool TextOrStage::isChildrenEOF(){
  for (size_t i = 0; i < _children.size(); ++i) {
      if(kChildIsEOF != _indexerStatus[i]) {
          // We have another child to read from.
          LOG(3) << "Is not EOF " << i << " " << _indexerStatus[i];
          return false;
      }
      LOG(3) << "Is EOF " << i << " " << _indexerStatus[i];
  }
  LOG(3) << "Is EOF " << _children.size();
  return true;
}

bool TextOrStage::processNextDoWork(){
    // Checking next 
    size_t isCheckingNextLength = _children.size();

    while(0 < isCheckingNextLength) {
        ++_currentChild;

        // If we out of range for _children - begin from first one
        if(_currentChild == _children.size()) {
            _currentChild = 0;
        }
        if(kChildIsEOF != _indexerStatus[_currentChild]) {
            LOG(3) << "next Child " << _currentChild << " state" << _indexerStatus[_currentChild];
            break;
        }
        --isCheckingNextLength;
    }
    if(0 == isCheckingNextLength) {
      LOG(3) << "All processed ";
      // Nothing left to process.
      return false;
    }
    LOG(3) << "process For Child " << _currentChild;
    _currentWorkState.wsid = WorkingSet::INVALID_ID;
    _currentWorkState.childStatus = _children[_currentChild]->work(&_currentWorkState.wsid);

    // Update stats counters.
    ++_specificStats.indexerCouter[_currentChild];
    if(kChildIsEOF != _indexerStatus[_currentChild]) {
      ++_indexerStatus[_currentChild];
    }
    return true;
}

PlanStage::StageState TextOrStage::readFromChildren(WorkingSetID* out) {
  LOG(3) << "stage readFromChildren";
    // Check to see if there were any children added in the first place.
    if (_children.size() == 0) {
        _internalState = State::kDone;
        return PlanStage::IS_EOF;
    }
    invariant(_currentChild < _children.size());


    if(!processNextDoWork()) {
      return PlanStage::IS_EOF;
    }

    
    if (PlanStage::ADVANCED == _currentWorkState.childStatus) {
        LOG(3) << "stage readFromChildren::ADVANCED";
        WorkingSetMember* member = _ws->get(_currentWorkState.wsid);
        // Maybe the child had an invalidation.  We intersect RecordId(s) so we can't do anything
        // with this WSM.
        if (!member->hasRecordId()) {
            _ws->flagForReview(_currentWorkState.wsid);
            return PlanStage::NEED_TIME;
        }
        ++_specificStats.dupsTested;
        if (!_wantTextScore) {
            if (_dataMap.end() != _dataMap.find(member->recordId)) {
                ++_specificStats.dupsDropped;
                _ws->free(_currentWorkState.wsid);
                return PlanStage::NEED_TIME;
            } else {
                TextRecordData textRecordData;
                textRecordData.wsid = _currentWorkState.wsid;
                if (!_dataMap.insert(std::make_pair(member->recordId, textRecordData)).second) {
                    // Didn't insert because we already had this RecordId inside the map. This
                    // should only
                    // happen if we're seeing a newer copy of the same doc in a more recent
                    // snapshot.
                    // Throw out the newer copy of the doc.
                    _ws->free(_currentWorkState.wsid);
                    return PlanStage::NEED_TIME;
                }
                *out = _currentWorkState.wsid;
                return PlanStage::ADVANCED;
            }
        }

        
        //auto& indexByRecordID = _dataIndexMap.get<IndexByRecordId>();
        

        double documentTermScore = getIndexScore(member);

        //auto itFound = indexByRecordID.find(member->recordId);
        //LOG(3) << "Count " << indexByRecordID.count(member->recordId);

        TextMapIndex::RecordIndex::iterator itC = _dataIndexMap.findByID(member->recordId);
        if(itC == _dataIndexMap.endRecords()) {
          TextMapIndex::IndexData recordData;
          recordData.recordId = member->recordId;
          recordData.wsid = _currentWorkState.wsid;
          recordData.score = documentTermScore;
          recordData.scoreTerms = std::vector<double>(_indexerStatus.size(), 0);
          recordData.scorePredictTerms = std::vector<double>(_indexerStatus.size(), 0);
          for (size_t i = 0; i < _scoreStatus.size(); ++i) {
            recordData.scorePredictTerms[i] = _scoreStatus[i];
          }
          recordData.scoreTerms[_currentChild] = documentTermScore;
          recordData.scorePredictTerms[_currentChild] = documentTermScore;
          _dataIndexMap.insert(recordData);
          LOG(3) << "Insert into TextMapIndex " << member->recordId << " " << documentTermScore;
        } else {
          const TextMapIndex::IndexData recordData = *itC;
          _dataIndexMap.update(itC, _currentChild, documentTermScore, _scoreStatus);
          LOG(3) << "Update  TextMapIndex " << member->recordId << " " << documentTermScore;
        }

        DataMap::iterator it = _dataMap.find(member->recordId);
        // Found. Store extra.
        if (_dataMap.end() != it) {
            it->second.score += documentTermScore;
            it->second.scoreTerms[_currentChild] = documentTermScore;
            /*++it->second._collectedNum;
            if(it->second._collectedNum == it->second.scoreTerms.size()) {
              it->second.collected = true;
            }*/

            // Validate if recordID is collected all terms.
            bool collected = true;
            for (size_t i = 0; i < it->second.scoreTerms.size(); ++i) {
                if(0 == it->second.scoreTerms[i]) {
                  collected = false;
                  break;
                }
            }
            if(collected) {
              it->second.collected = true;
            }

            ++_specificStats.dupsDropped;
            _ws->free(_currentWorkState.wsid);
            return PlanStage::NEED_TIME;
        }
        

        TextRecordData textRecordData;
        textRecordData.score = documentTermScore;
        textRecordData.wsid = _currentWorkState.wsid;
        // TODO: Maybe wrap into initialization of the variable.
        textRecordData.scoreTerms = std::vector<double>(_indexerStatus.size(), 0);
        textRecordData.scoreTerms[_currentChild] = documentTermScore;
        if (!_dataMap.insert(std::make_pair(member->recordId, textRecordData)).second) {
            LOG(3) << "that is should not happen " << member->recordId;
            // Didn't insert because we already had this RecordId inside the map. This should only
            // happen if we're seeing a newer copy of the same doc in a more recent snapshot.
            // Throw out the newer copy of the doc.
            _ws->free(_currentWorkState.wsid);
            return PlanStage::NEED_TIME;
        }
        // member->makeObjOwnedIfNeeded();
        return PlanStage::NEED_TIME;
    } else if (PlanStage::IS_EOF == _currentWorkState.childStatus) {
        LOG(3) << "stage readFromChildren::IS_EOF " << _currentChild;
        // Done with _currentChild, mark so.
        _indexerStatus[_currentChild] = kChildIsEOF;
        currentAllTermsScore -= _scoreStatus[_currentChild];
        _scoreStatus[_currentChild] = 0;
        LOG(3) << "set EOF " << _currentChild << _indexerStatus[_currentChild];

        // Check if we done with all children
        if(!isChildrenEOF()) {
          return PlanStage::NEED_TIME;
        }

        if (!_wantTextScore) {
            return PlanStage::IS_EOF;
        }

        _scoreIterator = _dataMap.begin();
        //_dataIndexMap.resetScopeIterator();
        // We need to sort _dataMap by score.
        _internalState = State::kReturningResults;

        return PlanStage::NEED_TIME;
    }
    LOG(3) << "stage readFromChildren::UNKNOWN";
    // NEED_TIME, ERROR, NEED_YIELD, pass them up.
    *out = _currentWorkState.wsid;
    return _currentWorkState.childStatus;
}

PlanStage::StageState TextOrStage::readFromChild(WorkingSetID* out) {
    LOG(3) << "stage readFromChild";
    if(!processNextDoWork()) {
      return PlanStage::IS_EOF;
    }
    
    if (PlanStage::ADVANCED == _currentWorkState.childStatus) {
        WorkingSetMember* member = _ws->get(_currentWorkState.wsid);
        // Maybe the child had an invalidation.  We intersect RecordId(s) so we can't do anything
        // with this WSM.
        if (!member->hasRecordId()) {
            _ws->flagForReview(_currentWorkState.wsid);
            return PlanStage::NEED_TIME;
        }

        if (!_wantTextScore) {
            *out = _currentWorkState.wsid;
            return PlanStage::ADVANCED;
        }

        TextRecordData textRecordData;
        textRecordData.score = getIndexScore(member);
        textRecordData.wsid = _currentWorkState.wsid;
        if (member->hasComputed(WSM_COMPUTED_TEXT_SCORE)) {
            member->updateComputed(new TextScoreComputedData(textRecordData.score));
        } else {
            member->addComputed(new TextScoreComputedData(textRecordData.score));
        }
    }

    // NEED_TIME, ERROR, NEED_YIELD, pass them up.
    *out = _currentWorkState.wsid;
    return _currentWorkState.childStatus;
}
PlanStage::StageState TextOrStage::returnReadyResults(WorkingSetID* out) {
    LOG(3) << "stage returnReadyResults";

    // If we already in kReturningResults, pass request there.
    if(_internalState == State::kReturningResults) {
      return PlanStage::IS_EOF;
    }

    _dataIndexMap.resetScopeIterator();

    if(_dataIndexMap.size() < 2) {
      LOG(3) << "_dataIndexMap size" <<  _dataIndexMap.size() ;
      return PlanStage::IS_EOF;
    }
    LOG(3) << "_dataIndexMap size: " <<  _dataIndexMap.size() ;
    if(_dataIndexMap.isScoreEmpty()) {
      LOG(3) << "_dataIndexMap isScoreEmpty" <<  _dataIndexMap.size() ;
      return PlanStage::IS_EOF;
    }

    LOG(3) << "currentAllTermsScore " << currentAllTermsScore ;
    if(0 == currentAllTermsScore) {
      //_dataIndexMap.scoreStepBack();
      return PlanStage::IS_EOF;
    }

    if(_predictScoreStatBase > 0 && _predictScoreDiff > 0) {
      if( _predictScoreStatBase - currentAllTermsScore < _predictScoreDiff) {
        LOG(3) << "_predictScoreStatBase " << _predictScoreStatBase
          << " _predictScoreDiff " << _predictScoreDiff
          << " currentAllTermsScore " << currentAllTermsScore
          << " current Diff " << (_predictScoreStatBase - currentAllTermsScore);
        //We still did not overcome a diff
        return PlanStage::IS_EOF;
      }
    } 
    
    TextMapIndex::IndexData recordData = _dataIndexMap.getScore();
    LOG(3) << "Found in TextMapIndex" 
            << "| recordID" << recordData.recordId 
            << "| wsid" << recordData.wsid 
            << "| score " << recordData.score;
      for (size_t i = 0; i < recordData.scoreTerms.size(); ++i) {
          LOG(3) << "| term " << i << " " << recordData.scoreTerms[i];
      }
    if( 0 == recordData.score) {
      return PlanStage::IS_EOF;
    }


    // Check if it is still possible to receive record that matcha ll terms and score better.
    if(recordData.score < currentAllTermsScore) {
      LOG(3) << "Possible max score record  " << currentAllTermsScore;
      return PlanStage::IS_EOF;
    }

    LOG(3) << "Currend Diff   " << recordData.score - currentAllTermsScore;
    

  // Count how many records with predict score > that currentAllTermsScore;
    TextMapIndex::ScorePredictIndex::iterator itScorePredict = _dataIndexMap.beginScorePredict();
    size_t predictCount = 0;
    while(true) {
      TextMapIndex::IndexData predictRecordData = *itScorePredict;
      ++itScorePredict;
      ++predictCount;
      if(predictRecordData.predictScore <= recordData.score) {
        break;
      }

      /*LOG(3) << "Found in TextMapIndex::predictRecordData " 
              << "| recordID " << predictRecordData.recordId 
              << "| wsid " << predictRecordData.wsid 
              << "| score " << predictRecordData.score
              << "| predictScore " << predictRecordData.predictScore
              << "| advanced " << predictRecordData.advanced;
        for (size_t i = 0; i < predictRecordData.scoreTerms.size(); ++i) {
            LOG(3) << "| term " << i << " " << predictRecordData.scoreTerms[i];
        }*/
      // Check if breaking
      double totalScoreDiff = recordData.score - predictRecordData.score;
      double expectedMaxScoreForSecond = 0;
      for (size_t i = 0; i < predictRecordData.scoreTerms.size(); ++i) {
        if(0 == predictRecordData.scoreTerms[i])  {
          expectedMaxScoreForSecond += _scoreStatus[i];
        }
      }
      if(totalScoreDiff < expectedMaxScoreForSecond) {
        // Our diff is smaller that possible changes.
        // break checking
        LOG(3) << "Break checking still out of order ";
        LOG(3) << "totalScoreDiff  " << totalScoreDiff
             << "expectedMaxScoreForSecond " << expectedMaxScoreForSecond;
        LOG(3) << "ScorePredict Count " << predictCount << " max " <<  _dataIndexMap.size() ;
        _predictScoreDiff = expectedMaxScoreForSecond - totalScoreDiff; 
        _predictScoreStatBase = currentAllTermsScore;
        return PlanStage::IS_EOF;
      }
      
      if(itScorePredict == _dataIndexMap.endScorePredict()){
        break;
      }
    }
    LOG(3) << "ScorePredict Count " << predictCount << " max " <<  _dataIndexMap.size() ;

// If we are here - we goof

    LOG(3) << "Advance " << recordData.wsid 
          << " ID " << recordData.recordId
          << " score " << recordData.score;
    _dataIndexMap.setAdvanced(recordData.recordId );
    WorkingSetMember* wsm = _ws->get(recordData.wsid);
    // Populate the working set member with the text score and return it.
    if (wsm->hasComputed(WSM_COMPUTED_TEXT_SCORE)) {
        wsm->updateComputed(new TextScoreComputedData(recordData.score));
    } else {
        wsm->addComputed(new TextScoreComputedData(recordData.score));
    }
    *out = recordData.wsid;
    return PlanStage::ADVANCED;
}

PlanStage::StageState TextOrStage::returnResults(WorkingSetID* out) {
    LOG(3) << "stage returnResults";
    LOG(3) << "stage End" << _dataIndexMap.size();

    if(_dataIndexMap.isScoreEmpty()) {
      _internalState = State::kDone;
      return PlanStage::IS_EOF;
    }

    TextMapIndex::IndexData textRecordData = _dataIndexMap.getScore();
    if(textRecordData.advanced) {
      // We reach to the list of advanced one
      _internalState = State::kDone;
      return PlanStage::IS_EOF;
    }
    _dataIndexMap.scoreStepForward();

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
