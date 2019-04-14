
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
const size_t TextOrStage::kReleaseEachNum = 1000;

TextOrStage::TextOrStage(OperationContext* opCtx,
                         WorkingSet* ws,
                         const FTSQueryImpl& query,
                         const FTSSpec& ftsSpec,
                         bool wantTextScore)
    : PlanStage(kStageType, opCtx),
      _ws(ws),
      _query(query),
      _ftsSpec(ftsSpec),
      _dataIndexVector(0),
      _currentChild(0),
      _indexerStatus(0),
      _scoreStatus(0),
      _wantTextScore(wantTextScore) {
    LOG(2) << "Freq " << _query.getFreq() ;
    _specificStats.wantTextScore = _wantTextScore;
    //_tmiScoreIterator = _dataIndexMap.beginScore();
    _dataIndexMap.resetScopeIterator();
    _dataIndexMap.reserve(_query.getFreq());
    _dataIndexVector.reserve(4000000);
    _dataIndexVector.reserve(4000001);
    LOG(1) << "_dataIndexVector " << _dataIndexVector.size() << " capacity " << _dataIndexVector.capacity() ;

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
        LOG(1) << "_debugCounter " << _debugCounter ;
        return PlanStage::IS_EOF;
    }

    PlanStage::StageState stageState = PlanStage::IS_EOF;
    // Optimization for one child to process
    if (1 == _children.size()) {
        _specificStats.singleChild = true;
        return readFromChild(out);
    }

    switch (_internalState) {
        case State::kReadingTerms: {
            // It's for score releasing only.
            if(_wantTextScore) {
              auto start = std::chrono::high_resolution_clock::now();
              stageState = returnReadyResults(out);
              auto elapsed = std::chrono::high_resolution_clock::now() - start;
              _debugCounter += std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
              if(stageState != PlanStage::IS_EOF) {
                return stageState;
              }
            }
            auto start2 = std::chrono::high_resolution_clock::now();
            stageState = readFromChildren(out);
            auto elapsed2 = std::chrono::high_resolution_clock::now() - start2;
            _debugCounterChild += std::chrono::duration_cast<std::chrono::microseconds>(elapsed2).count();
            
            break;
    }
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
    auto start = std::chrono::high_resolution_clock::now();
    if (member->hasComputed(WSM_COMPUTED_TEXT_SCORE)) {
        const TextScoreComputedData* score =
            static_cast<const TextScoreComputedData*>(member->getComputed(WSM_COMPUTED_TEXT_SCORE));
        currentAllTermsScore -= _scoreStatus[_currentChild];
        _scoreStatus[_currentChild] = score->getScore();
        //_scoreStatus[_currentChild] = (int)(_scoreStatus[_currentChild] * 100);
        //_scoreStatus[_currentChild] = (double)_scoreStatus[_currentChild] / 100;
        currentAllTermsScore += _scoreStatus[_currentChild];
    } else {
      const IndexKeyDatum newKeyData = member->keyData.back();

      BSONObjIterator keyIt(newKeyData.keyData);
      for (unsigned i = 0; i < _ftsSpec.numExtraBefore(); i++) {
          keyIt.next();
      }
      keyIt.next();  // Skip past 'term'.
      BSONElement scoreElement = keyIt.next();
      currentAllTermsScore -= _scoreStatus[_currentChild];
      _scoreStatus[_currentChild] = scoreElement.number();
      //_scoreStatus[_currentChild] = (int)(_scoreStatus[_currentChild] * 100);
      //_scoreStatus[_currentChild] = (double)_scoreStatus[_currentChild] / 100;
      currentAllTermsScore += _scoreStatus[_currentChild];
    }
    auto elapsed = std::chrono::high_resolution_clock::now() - start;
    _debugCounterScore += std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
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
    auto start2 = std::chrono::high_resolution_clock::now();
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
      auto elapsed2 = std::chrono::high_resolution_clock::now() - start2;
        _debugCounterChildWork += std::chrono::duration_cast<std::chrono::microseconds>(elapsed2).count();
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
    auto elapsed2 = std::chrono::high_resolution_clock::now() - start2;
        _debugCounterChildWork += std::chrono::duration_cast<std::chrono::microseconds>(elapsed2).count();
    return true;
}

PlanStage::StageState TextOrStage::readFromChildren(WorkingSetID* out) {
  LOG(3) << "stage readFromChildren";
  auto startReadFrom = std::chrono::high_resolution_clock::now();
    // Check to see if there were any children added in the first place.
    if (_children.size() == 0) {
        _internalState = State::kDone;
        LOG(1) << "_debugCounter " << _debugCounter ;
        return PlanStage::IS_EOF;
    }
    //invariant(_currentChild < _children.size());


    if(!processNextDoWork()) {
      LOG(1) << "_debugCounter " << _debugCounter ;
      return PlanStage::IS_EOF;
    }
    _debugCounterAfterNextDoWork += std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - startReadFrom).count();

    
    if (PlanStage::ADVANCED == _currentWorkState.childStatus) {
        LOG(3) << "stage readFromChildren::ADVANCED";
        WorkingSetMember* member = _ws->get(_currentWorkState.wsid);
        // Maybe the child had an invalidation.  We intersect RecordId(s) so we can't do anything
        // with this WSM.
        _debugCounterAfterGetWorker += std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - startReadFrom).count();
        if (!member->hasRecordId()) {
          
            _ws->flagForReview(_currentWorkState.wsid);
            _debugCounterAfterFlagForReview += std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - startReadFrom).count();
            return PlanStage::NEED_TIME;
        }

        ++_specificStats.dupsTested;
        if (!_wantTextScore) {
            TextMapIndex::RecordIndex::iterator itC = _dataIndexMap.findByID(member->recordId);
            if(itC != _dataIndexMap.endRecords()) {
                ++_specificStats.dupsDropped;
                _ws->free(_currentWorkState.wsid);
                return PlanStage::NEED_TIME;
            }
            _dataIndexVector.emplace_back(member->recordId,
                                        _currentWorkState.wsid,
                                        0,
                                        0,
                                        false,
                                        0,
                                        0
                                        );
          TextMapIndex::IndexData testRecordData = _dataIndexVector[_dataIndexVector.size() - 1];

            _dataIndexMap.insert(&testRecordData);
            _debugCounterInsert += std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - startReadFrom).count();
            LOG(3) << "stage ADVANCE";
            *out = _currentWorkState.wsid;
            return PlanStage::ADVANCED;
        }

        double documentTermScore = getIndexScore(member);
        
        TextMapIndex::RecordIndex::iterator itC = _dataIndexMap.findByID(member->recordId);
        _debugCounterAfterFindByID += std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - startReadFrom).count();
        if(itC == _dataIndexMap.endRecords()) {
          _debugCounterBeforeInsert += std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - startReadFrom).count();

          double PredictScore = documentTermScore;
          for (size_t i = 0; i < _scoreStatus.size(); ++i) {
            if(i != _currentChild) {
              PredictScore += _scoreStatus[i];
            }
            
          }
          _dataIndexVector.emplace_back(member->recordId,
                                        _currentWorkState.wsid,
                                        documentTermScore,
                                        PredictScore,
                                        false,
                                        _indexerStatus.size(),
                                        _indexerStatus.size()
                                        );
          TextMapIndex::IndexData testRecordData = _dataIndexVector[_dataIndexVector.size() - 1];
          for (size_t i = 0; i < _scoreStatus.size(); ++i) {
            testRecordData.scorePredictTerms[i] = _scoreStatus[i];
          }
          testRecordData.scoreTerms[_currentChild] = documentTermScore;
          testRecordData.scorePredictTerms[_currentChild] = documentTermScore;
          LOG(2) << "Found in testRecordData" 
            << "| recordID" << testRecordData.recordId 
            << "| wsid" << testRecordData.wsid 
            << "| score " << testRecordData.score
            << "| advanced " << testRecordData.advanced
            << "| score " << testRecordData.scoreTerms.size()
            << "| scorePredictTerms " << testRecordData.scorePredictTerms.size();
  
          _debugCounterAfterPrepareRecordData += std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - startReadFrom).count();
          //_dataIndexMap.insert(recordData);
          _dataIndexMap.insert(&testRecordData);
          _debugCounterInsert += std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - startReadFrom).count();
          LOG(3) << "Insert into TextMapIndex " << member->recordId << " " << documentTermScore;
        } else {
          ++_specificStats.dupsDropped;
          _debugCounterBeforeUpdate += std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - startReadFrom).count();
          const TextMapIndex::IndexData * recordData = *itC;
          LOG(2) << "Found in testRecordData" 
            << "| recordID" << recordData->recordId 
            << "| wsid" << recordData->wsid 
            << "| score " << recordData->score
            << "| advanced " << recordData->advanced
            << "| score " << recordData->scoreTerms.size()
            << "| scorePredictTerms " << recordData->scorePredictTerms.size();
          _dataIndexMap.update(itC, _currentChild, documentTermScore, _scoreStatus);
          _debugCounterUpdate += std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - startReadFrom).count();
          LOG(3) << "Update  TextMapIndex " << member->recordId << " " << documentTermScore;
          _debugCounterAfterLog += std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - startReadFrom).count();
        }
        _debugCounterProcessAdvance += std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - startReadFrom).count();

        *out = _currentWorkState.wsid;
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
        LOG(3) << "Finished on ";
        for (size_t i = 0; i < _scoreStatus.size(); ++i) {
          LOG(3) << " " << i << " " << _scoreStatus[i];
        }
        
        _debugCounterAfterEOF += std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - startReadFrom).count();

        LOG(1) << "_debugCounter " << _debugCounter ;
        LOG(1) << "_debugCounterScore " << _debugCounterScore ;
        LOG(1) << "_debugCounterChild " << _debugCounterChild ;
        LOG(1) << "_debugCounterChild2 " << _debugCounterChild2 ;
        LOG(1) << "_debugCounterBeforeInsert " << _debugCounterBeforeInsert;
        LOG(1) << "_debugCounterAfterPrepareRecordData " <<_debugCounterAfterPrepareRecordData;
        LOG(1) << "_debugCounterInsert " << _debugCounterInsert ;
        LOG(1) << "_debugCounterBeforeUpdate " <<_debugCounterBeforeUpdate;
        LOG(1) << "_debugCounterUpdate " << _debugCounterUpdate ;
        LOG(1) << "_debugCounterFind " << _debugCounterFind;
        LOG(1) << "_debugCounterNoRecords " << _debugCounterNoRecords;
        LOG(1) << "_debugCounterChildWork " << _debugCounterChildWork;
        LOG(1) << "_debugCounterAfterNextDoWork " << _debugCounterAfterNextDoWork;
        LOG(1) << "_debugCounterAfterGetWorker " << _debugCounterAfterGetWorker;
        LOG(1) << "_debugCounterAfterFlagForReview " << _debugCounterAfterFlagForReview;
        LOG(1) << "_debugCounterAfterFindByID " <<_debugCounterAfterFindByID;
        LOG(1) << "_debugCounterAfterLog " << _debugCounterAfterLog;
        LOG(1) << "_debugCounterProcessAdvance " << _debugCounterProcessAdvance;
        LOG(1) << "_debugCounterAfterEOF "<< _debugCounterAfterEOF;
        LOG(1) << "_dataIndexVector.size "<< _dataIndexVector.size();

        LOG(1) << "_debugCounterAfterReadFromChildren "<< _debugCounterAfterReadFromChildren;            
        if (!_wantTextScore) {
            _internalState = State::kDone;
            return PlanStage::IS_EOF;
        }

        //_dataIndexMap.resetScopeIterator();
        _tmiScoreIterator = _dataIndexMap.beginScore();

        _internalState = State::kReturningResults;

        return PlanStage::NEED_TIME;
    }
    _debugCounterAfterReadFromChildren += std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - startReadFrom).count();     
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

        double score = getIndexScore(member);
        if (member->hasComputed(WSM_COMPUTED_TEXT_SCORE)) {
            member->updateComputed(new TextScoreComputedData(score));
        } else {
            member->addComputed(new TextScoreComputedData(score));
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
    if(releaseEachNum < _query.getFreq() ) {
      LOG(2) << "Skip Releasing" <<  releaseEachNum << " " << _query.getFreq();
      releaseEachNum++;
      return PlanStage::IS_EOF;
    }


    //_dataIndexMap.resetScopeIterator();
    TextMapIndex::ScoreIndex::iterator itS = _dataIndexMap.beginScore();

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
      return PlanStage::IS_EOF;
    }

    if(_predictScoreStatBase > 0 && _predictScoreDiff > 0) {
      if( _predictScoreStatBase - currentAllTermsScore < _predictScoreDiff) {
        LOG(3) << "_predictScoreStatBase " << _predictScoreStatBase
          << " _predictScoreDiff " << _predictScoreDiff
          << " currentAllTermsScore " << currentAllTermsScore
          << " current Diff " << (_predictScoreStatBase - currentAllTermsScore);
        //We still did not overcome a diff
        releaseEachNum = 0;
        return PlanStage::IS_EOF;
      }
    } 
    
    TextMapIndex::IndexData * recordData = *itS;
    LOG(3) << "Found in returnReadyResults::TextMapIndex" 
            << "| recordID" << recordData->recordId 
            << "| wsid" << recordData->wsid 
            << "| score " << recordData->score;
      for (size_t i = 0; i < recordData->scoreTerms.size(); ++i) {
          LOG(3) << "| term " << i << " " << recordData->scoreTerms[i];
      }
    if( 0 == recordData->score) {
      return PlanStage::IS_EOF;
    }


    // Check if it is still possible to receive record that matcha ll terms and score better.
    if(recordData->score < currentAllTermsScore) {
      LOG(3) << "Possible max score record  " << currentAllTermsScore;
      return PlanStage::IS_EOF;
    }

    LOG(3) << "Currend Diff   " << recordData->score - currentAllTermsScore;
    

  // Count how many records with predict score > that currentAllTermsScore;
    TextMapIndex::ScorePredictIndex::iterator itScorePredict = _dataIndexMap.beginScorePredict();
    size_t predictCount = 0;
    while(true) {
      TextMapIndex::IndexData * predictRecordData = *itScorePredict;
      ++itScorePredict;
      ++predictCount;
      if(predictRecordData->predictScore <= recordData->score) {
        break;
      }

      LOG(3) << "Found in TextMapIndex::predictRecordData " 
              << "| recordID " << predictRecordData->recordId 
              << "| wsid " << predictRecordData->wsid 
              << "| score " << predictRecordData->score
              << "| predictScore " << predictRecordData->predictScore
              << "| advanced " << predictRecordData->advanced;
        for (size_t i = 0; i < predictRecordData->scoreTerms.size(); ++i) {
            LOG(3) << "| term " << i << " " << predictRecordData->scoreTerms[i];
        }
      // Check if breaking
      double totalScoreDiff = recordData->score - predictRecordData->score;
      double expectedMaxScoreForSecond = 0;
      for (size_t i = 0; i < predictRecordData->scoreTerms.size(); ++i) {
        if(0 == predictRecordData->scoreTerms[i])  {
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
        releaseEachNum = 0;
        return PlanStage::IS_EOF;
      } else {
        LOG(3) << "Skipping due to smaller actual";
        LOG(3) << "totalScoreDiff  " << totalScoreDiff
             << "expectedMaxScoreForSecond " << expectedMaxScoreForSecond;
        // Recalculate it.
        ++itScorePredict;
        _dataIndexMap.refreshScore(predictRecordData->recordId, _scoreStatus);
        --itScorePredict;

      }
      
      if(itScorePredict == _dataIndexMap.endScorePredict()){
        break;
      }
    }
    LOG(3) << "ScorePredict Count " << predictCount << " max " <<  _dataIndexMap.size() ;

// If we are here - we goof

    LOG(3) << "Advance " << recordData->wsid 
          << " ID " << recordData->recordId
          << " score " << recordData->score;
    _dataIndexMap.setAdvanced(recordData->recordId );
    WorkingSetMember* wsm = _ws->get(recordData->wsid);
    // Populate the working set member with the text score and return it.
    if (wsm->hasComputed(WSM_COMPUTED_TEXT_SCORE)) {
        wsm->updateComputed(new TextScoreComputedData(recordData->score));
    } else {
        wsm->addComputed(new TextScoreComputedData(recordData->score));
    }
    *out = recordData->wsid;
    return PlanStage::ADVANCED;
}

PlanStage::StageState TextOrStage::returnResults(WorkingSetID* out) {
    LOG(3) << "stage returnResults";
    LOG(3) << "stage End" << _dataIndexMap.size();

    //if(_dataIndexMap.isScoreEmpty()) {
    
    LOG(2) << "Get Score";
    //TextMapIndex::ScoreIndex::iterator itS = _dataIndexMap.beginScore();

    if(_tmiScoreIterator == _dataIndexMap.endScore()) {
      _internalState = State::kDone;
      LOG(2) << "_debugCounter " << _debugCounter ;
      return PlanStage::IS_EOF;
    }
    LOG(2) << "Get Data";
    TextMapIndex::IndexData * textRecordData = *_tmiScoreIterator;

    LOG(3) << "Found in returnResults:RecordData" 
            << "| recordID" << textRecordData->recordId 
            << "| wsid" << textRecordData->wsid 
            << "| score " << textRecordData->score
            << "| advanced " << textRecordData->advanced
            << "| score " << textRecordData->scoreTerms.size()
            << "| scorePredictTerms " << textRecordData->scorePredictTerms.size();

    if(textRecordData->advanced) {
      // We reach to the list of advanced one
      _internalState = State::kDone;
      LOG(2) << "_debugCounter " << _debugCounter ;
      return PlanStage::IS_EOF;
    }
    LOG(2) << "Step Forward";
    //_dataIndexMap.scoreStepForward();
    ++_tmiScoreIterator;



    WorkingSetMember* wsm = _ws->get(textRecordData->wsid);
    // Populate the working set member with the text score and return it.
    if (wsm->hasComputed(WSM_COMPUTED_TEXT_SCORE)) {
        wsm->updateComputed(new TextScoreComputedData(textRecordData->score));
    } else {
        wsm->addComputed(new TextScoreComputedData(textRecordData->score));
    }
    *out = textRecordData->wsid;
    return PlanStage::ADVANCED;
}

void TextOrStage::doInvalidate(OperationContext* opCtx, const RecordId& dl, InvalidationType type) {
    // TODO remove this since calling isEOF is illegal inside of doInvalidate().
    if (isEOF()) {
        return;
    }
    TextMapIndex::RecordIndex::iterator itC = _dataIndexMap.findByID(dl);
    if(itC != _dataIndexMap.endRecords()) {
        const TextMapIndex::IndexData recordData = **itC;
        WorkingSetID id = recordData.wsid;
        WorkingSetMember* member = _ws->get(id);
        verify(member->recordId == dl);
        _ws->flagForReview(id);
        ++_specificStats.recordIdsForgotten;
        // And don't return it from this stage.
        _dataIndexMap.erase(itC);
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
