
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

#include "mongo/db/exec/text.h"

#include <vector>

#include "mongo/db/exec/eof.h"
#include "mongo/db/exec/fetch.h"
#include "mongo/db/exec/filter.h"
#include "mongo/db/exec/index_scan.h"
#include "mongo/db/exec/or.h"
#include "mongo/db/exec/scoped_timer.h"
#include "mongo/db/exec/text_match.h"
#include "mongo/db/exec/text_or.h"
#include "mongo/db/exec/text_and.h"
#include "mongo/db/exec/text_nin.h"
#include "mongo/db/exec/working_set.h"
#include "mongo/db/fts/fts_index_format.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/query/internal_plans.h"
#include "mongo/stdx/memory.h"

namespace mongo {

using std::string;
using std::unique_ptr;
using std::vector;
using stdx::make_unique;

using stdx::make_unique;

using fts::FTSIndexFormat;
using fts::MAX_WEIGHT;

const char* TextStage::kStageType = "TEXT";

TextStage::TextStage(OperationContext* opCtx,
                     const TextStageParams& params,
                     WorkingSet* ws,
                     const MatchExpression* filter)
    : PlanStage(kStageType, opCtx), _params(params) {
    _children.emplace_back(buildTextTree(opCtx, ws, filter, params.wantTextScore));
    _specificStats.indexPrefix = _params.indexPrefix;
    _specificStats.indexName = _params.index->indexName();
    _specificStats.parsedTextQuery = _params.query.toBSON();
    _specificStats.textIndexVersion = _params.index->infoObj()["textIndexVersion"].numberInt();
}

bool TextStage::isEOF() {
    return child()->isEOF();
}

PlanStage::StageState TextStage::doWork(WorkingSetID* out) {
    if (isEOF()) {
        return PlanStage::IS_EOF;
    }

    return child()->work(out);
}

unique_ptr<PlanStageStats> TextStage::getStats() {
    _commonStats.isEOF = isEOF();

    unique_ptr<PlanStageStats> ret = make_unique<PlanStageStats>(_commonStats, STAGE_TEXT);
    ret->specific = make_unique<TextStats>(_specificStats);
    ret->children.emplace_back(child()->getStats());
    return ret;
}

const SpecificStats* TextStage::getSpecificStats() const {
    return &_specificStats;
}

unique_ptr<PlanStage> TextStage::buildTextTree(OperationContext* opCtx,
                                               WorkingSet* ws,
                                               const MatchExpression* filter,
                                               bool wantTextScore) const {
    // Get all the index scans for each term in our query.
    std::vector<std::unique_ptr<PlanStage>> indexScanList;

    // return EOF stage for when no positive terms provided.
    std::set<std::string> termsForBounds = _params.query.getTermsForBounds();
    if(0 == termsForBounds.size()) {
      auto eofStage = make_unique<EOFStage>(
          opCtx);
      return eofStage;
    }

    std::unique_ptr<PlanStage> textMatchStage;

    // Build fetching plan based on _params.query
    if (!wantTextScore) {
        //First we need to retrive positive phrase indexes
        std::vector<std::set<std::string>> positivePhrasesBounds = _params.query.getTermsPhrasesForBounds();
        auto textORSearcher = make_unique<OrStage>(opCtx, ws, true, filter);
        std::vector<std::unique_ptr<PlanStage>> indexORScanList;

        // We have positive phrases, retrive them separately 
        if(0 < positivePhrasesBounds.size()) {
          
            // Create stage TextAnd for each phrase 
            for (size_t i = 0; i < positivePhrasesBounds.size(); i++) {
                std::set<std::string> andTerms = positivePhrasesBounds[i];
                std::vector<std::unique_ptr<PlanStage>> indexAndScanList;

                // OPTIMIZATION: If phrase is single term, add it directly ot TEXT_OR
                if (1 == andTerms.size()) {
                    std::set<std::string>::const_iterator setIterator = andTerms.begin();
                    const auto& term = *setIterator;
                    IndexScanParams ixparams;

                    ixparams.bounds.startKey = FTSIndexFormat::getIndexKey(
                        MAX_WEIGHT, term, _params.indexPrefix, _params.spec.getTextIndexVersion());
                    ixparams.bounds.endKey = FTSIndexFormat::getIndexKey(
                        0, term, _params.indexPrefix, _params.spec.getTextIndexVersion());
                    ixparams.bounds.boundInclusion = BoundInclusion::kIncludeBothStartAndEndKeys;
                    ixparams.bounds.isSimpleRange = true;
                    ixparams.descriptor = _params.index;
                    ixparams.direction = -1;

                    indexORScanList.push_back(
                        stdx::make_unique<IndexScan>(opCtx, ixparams, ws, nullptr));
                } else {
                    // scan index for each term separately
                    for (const auto& term : andTerms) {
                        IndexScanParams ixparams;

                        ixparams.bounds.startKey = FTSIndexFormat::getIndexKey(
                            MAX_WEIGHT, term, _params.indexPrefix, _params.spec.getTextIndexVersion());
                        ixparams.bounds.endKey = FTSIndexFormat::getIndexKey(
                            0, term, _params.indexPrefix, _params.spec.getTextIndexVersion());
                        ixparams.bounds.boundInclusion = BoundInclusion::kIncludeBothStartAndEndKeys;
                        ixparams.bounds.isSimpleRange = true;
                        ixparams.descriptor = _params.index;
                        ixparams.direction = -1;

                        indexAndScanList.push_back(
                            stdx::make_unique<IndexScan>(opCtx, ixparams, ws, nullptr));
                    }
                    // Add TextAndStage to OR childrenList

                    indexORScanList.push_back(stdx::make_unique<TextAndStage>(
                        opCtx, ws, _params.spec, wantTextScore, std::move(indexAndScanList)));
                }
            }
            // Add single terms that did not match into prases
            for (const auto& term : _params.query.getTermsOutOfPhrasesForBounds()) {
                IndexScanParams ixparams;

                ixparams.bounds.startKey = FTSIndexFormat::getIndexKey(
                    MAX_WEIGHT, term, _params.indexPrefix, _params.spec.getTextIndexVersion());
                ixparams.bounds.endKey = FTSIndexFormat::getIndexKey(
                    0, term, _params.indexPrefix, _params.spec.getTextIndexVersion());
                ixparams.bounds.boundInclusion = BoundInclusion::kIncludeBothStartAndEndKeys;
                ixparams.bounds.isSimpleRange = true;
                ixparams.descriptor = _params.index;
                ixparams.direction = -1;

                indexORScanList.push_back(stdx::make_unique<IndexScan>(opCtx, ixparams, ws, nullptr));
            }
            textORSearcher->addChildren(std::move(indexORScanList));
        } else {
            for (const auto& term : _params.query.getTermsForBounds()) {
                IndexScanParams ixparams;

                ixparams.bounds.startKey = FTSIndexFormat::getIndexKey(
                    MAX_WEIGHT, term, _params.indexPrefix, _params.spec.getTextIndexVersion());
                ixparams.bounds.endKey = FTSIndexFormat::getIndexKey(
                    0, term, _params.indexPrefix, _params.spec.getTextIndexVersion());
                ixparams.bounds.boundInclusion = BoundInclusion::kIncludeBothStartAndEndKeys;
                ixparams.bounds.isSimpleRange = true;
                ixparams.descriptor = _params.index;
                ixparams.direction = -1;

                indexORScanList.push_back(stdx::make_unique<IndexScan>(opCtx, ixparams, ws, nullptr));
            }
            textORSearcher->addChildren(std::move(indexORScanList));
        }

        std::set<std::string> negativeTerms = _params.query.getNegatedTerms();
        if(0 < negativeTerms.size()) {
          std::vector<std::unique_ptr<PlanStage>> indexNINScanList;
          for (const auto& term : _params.query.getNegatedTerms()) {
            IndexScanParams ixparams;

            ixparams.bounds.startKey = FTSIndexFormat::getIndexKey(
                MAX_WEIGHT, term, _params.indexPrefix, _params.spec.getTextIndexVersion());
            ixparams.bounds.endKey = FTSIndexFormat::getIndexKey(
                0, term, _params.indexPrefix, _params.spec.getTextIndexVersion());
            ixparams.bounds.boundInclusion = BoundInclusion::kIncludeBothStartAndEndKeys;
            ixparams.bounds.isSimpleRange = true;
            ixparams.descriptor = _params.index;
            ixparams.direction = -1;

            indexNINScanList.push_back(stdx::make_unique<IndexScan>(opCtx, ixparams, ws, nullptr));
          }
          auto textNINStage = make_unique<TextNINStage>(
            opCtx, ws, textORSearcher.release(), std::move(indexNINScanList));

          const MatchExpression* emptyFilter = nullptr;
          auto fetchStage = make_unique<FetchStage>(
              opCtx, ws, textNINStage.release(), emptyFilter, _params.index->getCollection());

          textMatchStage = make_unique<TextMatchStage>(
              opCtx, std::move(fetchStage), _params.query, _params.spec, ws, true);
          return textMatchStage;
        }
        const MatchExpression* emptyFilter = nullptr;
        auto fetchStage = make_unique<FetchStage>(
            opCtx, ws, textORSearcher.release(), emptyFilter, _params.index->getCollection());

        textMatchStage = make_unique<TextMatchStage>(
            opCtx, std::move(fetchStage), _params.query, _params.spec, ws, true);
        return textMatchStage;
    }

    for (const auto& term : _params.query.getTermsForBounds()) {
        IndexScanParams ixparams;

        ixparams.bounds.startKey = FTSIndexFormat::getIndexKey(
            MAX_WEIGHT, term, _params.indexPrefix, _params.spec.getTextIndexVersion());
        ixparams.bounds.endKey = FTSIndexFormat::getIndexKey(
            0, term, _params.indexPrefix, _params.spec.getTextIndexVersion());
        ixparams.bounds.boundInclusion = BoundInclusion::kIncludeBothStartAndEndKeys;
        ixparams.bounds.isSimpleRange = true;
        ixparams.descriptor = _params.index;
        ixparams.direction = -1;

        indexScanList.push_back(stdx::make_unique<IndexScan>(opCtx, ixparams, ws, nullptr));
    }

    // Build the union of the index scans as a TEXT_OR or an OR stage, depending on whether the
    // projection requires the "textScore" $meta field.
    
    if (wantTextScore) {
        // We use a TEXT_OR stage to get the union of the results from the index scans and then
        // compute their text scores. This is a blocking operation.
        auto textScorer = make_unique<TextOrStage>(opCtx, _params.spec, ws, filter, _params.index);

        textScorer->addChildren(std::move(indexScanList));

        textMatchStage = make_unique<TextMatchStage>(
            opCtx, std::move(textScorer), _params.query, _params.spec, ws);
    } else {
        // Because we don't need the text score, we can use a non-blocking OR stage to get the union
        // of the index scans.
        auto textSearcher = make_unique<OrStage>(opCtx, ws, true, filter);

        textSearcher->addChildren(std::move(indexScanList));

        // Unlike the TEXT_OR stage, the OR stage does not fetch the documents that it outputs. We
        // add our own FETCH stage to satisfy the requirement of the TEXT_MATCH stage that its
        // WorkingSetMember inputs have fetched data.
        const MatchExpression* emptyFilter = nullptr;
        auto fetchStage = make_unique<FetchStage>(
            opCtx, ws, textSearcher.release(), emptyFilter, _params.index->getCollection());

        textMatchStage = make_unique<TextMatchStage>(
            opCtx, std::move(fetchStage), _params.query, _params.spec, ws);
    }

    return textMatchStage;
}

}  // namespace mongo
