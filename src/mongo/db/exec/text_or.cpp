
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


#include "mongo/db/exec/text_or.h"

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
const char* TextOrStage::kStageType = "TEXT_OR";
const size_t TextOrStage::kChildIsEOF = -1;
const size_t TextOrStage::kMinReserve = 1000;

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

    _dataIndexMap.resetScopeIterator();
    _reserved = kMinReserve;
    _dataIndexMap.reserve(_reserved);
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
            // It's for score releasing only.
            if (_wantTextScore) {
                stageState = returnReadyResults(out);
                if (stageState != PlanStage::IS_EOF) {
                    return stageState;
                }
            }
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

bool TextOrStage::isChildrenEOF() {
    for (size_t i = 0; i < _children.size(); ++i) {
        if (kChildIsEOF != _indexerStatus[i]) {
            return false;
        }
    }
    return true;
}

bool TextOrStage::processNextDoWork() {
    // Checking next
    size_t isCheckingNextLength = _children.size();

    while (0 < isCheckingNextLength) {
        ++_currentChild;

        // If we out of range for _children - begin from first one
        if (_currentChild == _children.size()) {
            _currentChild = 0;
        }
        if (kChildIsEOF != _indexerStatus[_currentChild]) {
            break;
        }
        --isCheckingNextLength;
    }
    if (0 == isCheckingNextLength) {
        // Nothing left to process.
        return false;
    }
    _currentWorkState.wsid = WorkingSet::INVALID_ID;
    _currentWorkState.childStatus = _children[_currentChild]->work(&_currentWorkState.wsid);

    if (kChildIsEOF != _indexerStatus[_currentChild]) {
        ++_indexerStatus[_currentChild];
    }
    return true;
}

PlanStage::StageState TextOrStage::readFromChildren(WorkingSetID* out) {
    // Check to see if there were any children added in the first place.
    if (_children.size() == 0) {
        _internalState = State::kDone;
        return PlanStage::IS_EOF;
    }
    invariant(_currentChild < _children.size());


    if (!processNextDoWork()) {
        return PlanStage::IS_EOF;
    }


    if (PlanStage::ADVANCED == _currentWorkState.childStatus) {
        WorkingSetMember* member = _ws->get(_currentWorkState.wsid);
        // Maybe the child had an invalidation.
        // We intersect RecordId(s) so we can't do anything
        // with this WSM.
        if (!member->hasRecordId()) {
            _ws->flagForReview(_currentWorkState.wsid);
            return PlanStage::NEED_TIME;
        }
        ++_specificStats.dupsTested;

        // Incriase reserve for speed performance
        if (_reserved < _dataIndexMap.size()) {
            _reserved += _dataIndexMap.size() * _children.size();
            _dataIndexMap.reserve(_reserved);
        }

        if (!_wantTextScore) {
            TextMapIndex::RecordIndex::iterator itC = _dataIndexMap.findByID(member->recordId);
            if (itC != _dataIndexMap.endRecords()) {
                ++_specificStats.dupsDropped;
                _ws->free(_currentWorkState.wsid);
                return PlanStage::NEED_TIME;
            }

            _dataIndexMap.emplace(member->recordId, _currentWorkState.wsid);

            // Update stats counters.
            ++_specificStats.indexerCouter[_currentChild];
            *out = _currentWorkState.wsid;
            return PlanStage::ADVANCED;
        }

        double documentTermScore = getIndexScore(member);

        TextMapIndex::RecordIndex::iterator itC = _dataIndexMap.findByID(member->recordId);
        if (itC == _dataIndexMap.endRecords()) {

            TextMapIndex::ScoreStorage scoreTerms = TextMapIndex::ScoreStorage();
            TextMapIndex::ScoreStorage scorePredictTerms = TextMapIndex::ScoreStorage();
            double PredictScore = 0;
            for (size_t i = 0; i < _scoreStatus.size(); ++i) {
                if (i != _currentChild) {
                    PredictScore += _scoreStatus[i];
                    scorePredictTerms.push_back(_scoreStatus[i]);
                    scoreTerms.push_back(0);
                } else {
                    scorePredictTerms.push_back(documentTermScore);
                    scoreTerms.push_back(documentTermScore);
                }
            }
            _dataIndexMap.emplace(member->recordId,
                                  _currentWorkState.wsid,
                                  documentTermScore,
                                  PredictScore,
                                  false,
                                  scoreTerms,
                                  scorePredictTerms);


        } else {
            ++_specificStats.dupsDropped;
            _dataIndexMap.update(itC, _currentChild, documentTermScore, _scoreStatus);
        }
        *out = _currentWorkState.wsid;
        return PlanStage::NEED_TIME;
    } else if (PlanStage::IS_EOF == _currentWorkState.childStatus) {
        // Done with _currentChild, mark so.
        _indexerStatus[_currentChild] = kChildIsEOF;
        currentAllTermsScore -= _scoreStatus[_currentChild];
        _scoreStatus[_currentChild] = 0;

        // Check if we done with all children
        if (!isChildrenEOF()) {
            return PlanStage::NEED_TIME;
        }

        _dataIndexMap.resetScopeIterator();

        if (!_wantTextScore) {
            _internalState = State::kDone;
            return PlanStage::IS_EOF;
        }

        _internalState = State::kReturningResults;

        return PlanStage::NEED_TIME;
    }
    // NEED_TIME, ERROR, NEED_YIELD, pass them up.
    *out = _currentWorkState.wsid;
    return _currentWorkState.childStatus;
}

PlanStage::StageState TextOrStage::readFromChild(WorkingSetID* out) {
    if (!processNextDoWork()) {
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
        // Update stats counters.
        ++_specificStats.indexerCouter[_currentChild];
        if (!_wantTextScore) {
            *out = _currentWorkState.wsid;
            return PlanStage::ADVANCED;
        }

        double documentTermScore = getIndexScore(member);
        if (member->hasComputed(WSM_COMPUTED_TEXT_SCORE)) {
            member->updateComputed(new TextScoreComputedData(documentTermScore));
        } else {
            member->addComputed(new TextScoreComputedData(documentTermScore));
        }
    }

    // NEED_TIME, ERROR, NEED_YIELD, pass them up.
    *out = _currentWorkState.wsid;
    return _currentWorkState.childStatus;
}
PlanStage::StageState TextOrStage::returnReadyResults(WorkingSetID* out) {

    // If we already in kReturningResults, pass request there.
    if (_internalState == State::kReturningResults) {
        return PlanStage::IS_EOF;
    }


    if (_predictScoreStatBase > 0 && _predictScoreDiff > 0) {
        if (_predictScoreStatBase - currentAllTermsScore < _predictScoreDiff) {
            // We still did not overcome a diff
            return PlanStage::IS_EOF;
        }
    }


    _dataIndexMap.resetScopeIterator();

    if (_dataIndexMap.size() < 2) {
        return PlanStage::IS_EOF;
    }
    if (_dataIndexMap.isScoreEmpty()) {
        return PlanStage::IS_EOF;
    }

    if (0 == currentAllTermsScore) {
        return PlanStage::IS_EOF;
    }


    TextMapIndex::IndexData recordData = _dataIndexMap.getScore();
    if (0 == recordData.score) {
        return PlanStage::IS_EOF;
    }

    // Check if it is still possible to receive record that match all terms and score better.
    if (recordData.score < currentAllTermsScore) {
        return PlanStage::IS_EOF;
    }


    // Count how many records with predict score > that currentAllTermsScore;
    TextMapIndex::ScorePredictIndex::iterator itScorePredict = _dataIndexMap.beginScorePredict();
    size_t predictCount = 0;
    while (true) {
        TextMapIndex::IndexData predictRecordData = *itScorePredict;
        ++itScorePredict;
        ++predictCount;
        if (predictRecordData.predictScore <= recordData.score) {
            break;
        }

        // Check if breaking
        double totalScoreDiff = recordData.score - predictRecordData.score;
        double expectedMaxScoreForSecond = 0;
        for (size_t i = 0; i < predictRecordData.scoreTerms.size(); ++i) {
            if (0 == predictRecordData.scoreTerms[i]) {
                expectedMaxScoreForSecond += _scoreStatus[i];
            }
        }
        if (totalScoreDiff < expectedMaxScoreForSecond) {
            _predictScoreDiff = expectedMaxScoreForSecond - totalScoreDiff;
            _predictScoreStatBase = currentAllTermsScore;
            return PlanStage::IS_EOF;
        } else {
            // Recalculate predict score for this record.
            ++itScorePredict;
            _dataIndexMap.refreshScore(predictRecordData.recordId, _scoreStatus);
            --itScorePredict;
        }

        if (itScorePredict == _dataIndexMap.endScorePredict()) {
            break;
        }
    }

    // If we are here - we good to advance this record.

    _dataIndexMap.setAdvanced(recordData.recordId);
    WorkingSetMember* wsm = _ws->get(recordData.wsid);
    // Populate the working set member with the text score and return it.
    if (wsm->hasComputed(WSM_COMPUTED_TEXT_SCORE)) {
        wsm->updateComputed(new TextScoreComputedData(recordData.score));
    } else {
        wsm->addComputed(new TextScoreComputedData(recordData.score));
    }
    // Update stats counters.
    ++_specificStats.indexerCouter[_currentChild];
    *out = recordData.wsid;
    return PlanStage::ADVANCED;
}

PlanStage::StageState TextOrStage::returnResults(WorkingSetID* out) {
    if (_dataIndexMap.isScoreEmpty()) {
        _internalState = State::kDone;
        return PlanStage::IS_EOF;
    }

    TextMapIndex::IndexData textRecordData = _dataIndexMap.getScore();
    if (textRecordData.advanced) {
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
    TextMapIndex::RecordIndex::iterator itC = _dataIndexMap.findByID(dl);
    if (itC != _dataIndexMap.endRecords()) {
        const TextMapIndex::IndexData recordData = *itC;
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
