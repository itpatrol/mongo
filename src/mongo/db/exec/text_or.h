
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
 *    TODO: see if we can update it
 */

#pragma once

#include <vector>

#include "mongo/db/exec/plan_stage.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/fts/fts_spec.h"
#include "mongo/db/record_id.h"
#include "mongo/platform/unordered_map.h"
#include "mongo/platform/unordered_set.h"

namespace mongo {

  using fts::FTSSpec;

/**
 * This stage outputs the union of its children.  It optionally deduplicates on RecordId.
 *
 * Preconditions: Valid RecordId.
 *
 * If we're deduping, we may fail to dedup any invalidated RecordId properly.
 */
class TextOrStage final : public PlanStage {
public:
    /**
     * Internal states.
     */
    enum class State {
        // 1. Read from previos stages
        kReadingTerms,

        // 2. Return results to our parent.
        kReturningResults,

        // 3. Finished.
        kDone,
    };
    TextOrStage(OperationContext* opCtx,
                 WorkingSet* ws,
                 const FTSSpec& ftsSpec,
                 bool wantTextScore);

    void addChild(PlanStage* child);

    void addChildren(Children childrenToAdd);

    bool isEOF() final;

    StageState doWork(WorkingSetID* out) final;

    void doInvalidate(OperationContext* opCtx, const RecordId& dl, InvalidationType type) final;

    StageType stageType() const final {
        return STAGE_TEXT_OR;
    }

    std::unique_ptr<PlanStageStats> getStats() final;

    const SpecificStats* getSpecificStats() const final;

    static const char* kStageType;

private:
     /**
     * Worker for Single CHild. Reads from the children, searching for the terms in the query and
     * populates the score map.
     */
    StageState readFromChild(WorkingSetID* out);
    /**
     * Worker for kReadingTerms. Reads from the children, searching for the terms in the query and
     * populates the score map.
     */
    StageState readFromChildren(WorkingSetID* out);

    /**
     * Worker for kReturningResults. Returns a wsm with RecordID and Score.
     */
    StageState returnResults(WorkingSetID* out);

    /**
     * Retrive score from previos stage.
     */
    double getIndexScore(WorkingSetMember* member);

    // Not owned by us.
    WorkingSet* _ws;

    // The index spec used to determine where to find the score.
    FTSSpec _ftsSpec;
    

    struct TextRecordData {
        TextRecordData() : wsid(WorkingSet::INVALID_ID), score(0.0) {}
        WorkingSetID wsid;
        double score;
    };

    // _dataMap is filled out by the first child and probed by subsequent children.  This is the
    // hash table that we create by intersecting _children and probe with the last child.
    typedef unordered_map<RecordId, TextRecordData, RecordId::Hasher> DataMap;
    DataMap _dataMap;

    DataMap::const_iterator _scoreIterator;

    // Keeps track of what elements from _dataMap subsequent children have seen.
    // Only used while _hashingChildren.
    //typedef unordered_set<RecordId, RecordId::Hasher> SeenMap;
    //SeenMap _seenMap;

    // What state are we in?  See the State enum above.
    State _internalState = State::kReadingTerms;

    // Which of _children are we calling work(...) on now?
    size_t _currentChild = 0;

    // Track the status of the child work progress
    // 0-N - number of processed items
    // -1 - mean EOF from the child
    std::vector<size_t> _indexerStatus;

    // True if we dedup on RecordId, false otherwise.
    bool _wantTextScore;

    // Stats
    TextOrStats _specificStats;
};

}  // namespace mongo
