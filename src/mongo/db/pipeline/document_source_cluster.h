/**
 * Copyright (C) 2016 MongoDB Inc.
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects
 * for all of the code used other than as permitted herein. If you modify
 * file(s) with this exception, you may extend this exception to your
 * version of the file(s), but you are not obligated to do so. If you do not
 * wish to do so, delete this exception statement from your version. If you
 * delete this exception statement from all source files in the program,
 * then also delete it in the license file.
 */

#pragma once

#include "mongo/db/pipeline/accumulation_statement.h"
#include "mongo/db/pipeline/accumulator.h"
#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/pipeline/expression.h"
#include "mongo/db/sorter/sorter.h"

namespace mongo {

/**
 * The $cluster stage takes user specified field with location and distance and group locations by it.
 */
class DocumentSourceCluster final : public DocumentSource, public SplittableDocumentSource {
public:
    Value serialize(bool explain = false) const final;
    GetDepsReturn getDependencies(DepsTracker* deps) const final;
    GetNextResult getNext() final;
    void dispose() final;
    const char* getSourceName() const final;

    /**
     * The $cluster stage must be run on the merging shard.
     */
    boost::intrusive_ptr<DocumentSource> getShardSource() final {
        return nullptr;
    }
    boost::intrusive_ptr<DocumentSource> getMergeSource() final {
        return this;
    }

    static const uint64_t kDefaultMaxMemoryUsageBytes = 100 * 1024 * 1024;

    /**
     * Convenience method to create a $cluster stage.
     *
     * If 'accumulationStatements' is the empty vector, it will be filled in with the statement
     * 'count: {$sum: 1}'.
     */
    static boost::intrusive_ptr<DocumentSourceCluster> create(
        const boost::intrusive_ptr<ExpressionContext>& expCtx,
        const boost::intrusive_ptr<Expression>& locationExpression,
        Variables::Id numVariables,
        double lonDelta,
        double latDelta,
        std::vector<AccumulationStatement> accumulationStatements = {},
        uint64_t maxMemoryUsageBytes = kDefaultMaxMemoryUsageBytes);

    /**
     * Parses a $cluster stage from the user-supplied BSON.
     */
    static boost::intrusive_ptr<DocumentSource> createFromBson(
        BSONElement elem, const boost::intrusive_ptr<ExpressionContext>& pExpCtx);

private:
    DocumentSourceCluster(const boost::intrusive_ptr<ExpressionContext>& pExpCtx,
                          const boost::intrusive_ptr<Expression>& locationExpression,
                          Variables::Id numVariables,
                          double lonDelta,
                          double latDelta,
                          std::vector<AccumulationStatement> accumulationStatements,
                          uint64_t maxMemoryUsageBytes);

    // struct for holding information about a bucket.
    struct Bucket {
        Bucket(const boost::intrusive_ptr<ExpressionContext>& expCtx,
                double Longitude,
                double Latitude,
                std::vector<Accumulator::Factory> accumulatorFactories);
        double _Longitude;
        double _Latitude;
        std::vector<boost::intrusive_ptr<Accumulator>> _accums;
    };

    /**
     * Consumes all of the documents from the source in the pipeline and sorts them by their
     * 'location' value. This method might not be able to finish populating the sorter in a single
     * call if 'pSource' returns a DocumentSource::GetNextResult::kPauseExecution, so this returns
     * the last GetNextResult encountered, which may be either kEOF or kPauseExecution.
     */
    GetNextResult populateSorter();

    /**
     * Computes the 'location' expression value for 'doc'.
     */
    Value extractKey(const Document& doc);

    /**
     * Adds the document in 'entry' to 'bucket' by updating the accumulators in 'bucket'.
     */
    void addDocumentToBucket(const std::pair<Value, Document>& entry, Bucket& bucket);

    /**
     * Adds 'newBucket' to _buckets and updates any boundaries if necessary.
     */
    void addBucket(Bucket& newBucket);

    /**
     * Makes a document using the information from bucket. This is what is returned when getNext()
     * is called.
     */
    Document makeDocument(const Bucket& bucket);

    std::unique_ptr<Sorter<Value, Document>> _sorter;
    std::unique_ptr<Sorter<Value, Document>::Iterator> _sortedInput;

    // _fieldNames contains the field names for the result documents, _accumulatorFactories contains
    // the accumulator factories for the result documents, and _expressions contains the common
    // expressions used by each instance of each accumulator in order to find the right-hand side of
    // what gets added to the accumulator. These three vectors parallel each other.
    std::vector<std::string> _fieldNames;
    std::vector<Accumulator::Factory> _accumulatorFactories;
    std::vector<boost::intrusive_ptr<Expression>> _expressions;

    int _nBuckets = 0;
    double _lonDelta;
    double _latDelta;
    uint64_t _maxMemoryUsageBytes;
    bool _populated = false;
    std::vector<Bucket> _buckets;
    std::vector<Bucket>::iterator _bucketsIterator;
    std::unique_ptr<Variables> _variables;
    boost::intrusive_ptr<Expression> _locationExpression;
    long long _nDocuments = 0;
};

}  // namespace mongo