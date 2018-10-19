/**
 *    Copyright (C) 2016 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "mongo/db/pipeline/document_source_cluster.h"

#include "mongo/db/pipeline/accumulation_statement.h"
#include "mongo/db/pipeline/lite_parsed_document_source.h"
#include "mongo/util/log.h"

namespace mongo {

using boost::intrusive_ptr;
using std::pair;
using std::string;
using std::vector;

REGISTER_DOCUMENT_SOURCE(cluster,
                          LiteParsedDocumentSourceDefault::parse,
                          DocumentSourceCluster::createFromBson);


const char* DocumentSourceCluster::getSourceName() const {
    return "$cluster";
}

DocumentSource::GetNextResult DocumentSourceCluster::getNext() {
    pExpCtx->checkForInterrupt();

    if (!_populated) {
        const auto populationResult = populateSorter();
        if (populationResult.isPaused()) {
            return populationResult;
        }
        invariant(populationResult.isEOF());

        populateBuckets();

        _populated = true;
        _bucketsIterator = _buckets.begin();
    }

    if (_bucketsIterator == _buckets.end()) {
        dispose();
        return GetNextResult::makeEOF();
    }

    return makeDocument(*(_bucketsIterator++));
}

// TODO: Add logic here
DocumentSource::GetDepsReturn DocumentSourceCluster::getDependencies(DepsTracker* deps) const {
    // Add the 'groupBy' expression.
    _locationExpression->addDependencies(deps);

    // Add the 'output' fields.
    for (auto&& exp : _expressions) {
        exp->addDependencies(deps);
    }

    // We know exactly which fields will be present in the output document. Future stages cannot
    // depend on any further fields. The grouping process will remove any metadata from the
    // documents, so there can be no further dependencies on metadata.
    return EXHAUSTIVE_ALL;
}


DocumentSource::GetNextResult DocumentSourceCluster::populateSorter() {
    if (!_sorter) {
        SortOptions opts;
        opts.maxMemoryUsageBytes = _maxMemoryUsageBytes;
        if (pExpCtx->extSortAllowed && !pExpCtx->inRouter) {
            opts.extSortAllowed = true;
            opts.tempDir = pExpCtx->tempDir;
        }
        const auto& valueCmp = pExpCtx->getValueComparator();
        auto comparator = [valueCmp](const Sorter<Value, Document>::Data& lhs,
                                     const Sorter<Value, Document>::Data& rhs) {
            return valueCmp.compare(lhs.first, rhs.first);
        };

        _sorter.reset(Sorter<Value, Document>::make(opts, comparator));
    }

    auto next = pSource->getNext();
    for (; next.isAdvanced(); next = pSource->getNext()) {
        auto nextDoc = next.releaseDocument();
        _sorter->add(extractKey(nextDoc), nextDoc);
        _nDocuments++;
    }
    return next;
}


Value DocumentSourceCluster::extractKey(const Document& doc) {
    if (!_locationExpression) {
        return Value(BSONNULL);
    }

    _variables->setRoot(doc);
    Value key = _locationExpression->evaluate(_variables.get());
    uassert(40507,
                str::stream() << "$cluster can specify a 'location' with array "
                                 "only, but found a value with type: "
                              << typeName(key.getType()),
                key.getType() == Array);
    const vector<Value>& vec = key.getArray();
    LOG(3) << "key :" << key ;
    uassert(40508,
                str::stream() << "$cluster can specify a 'location' with array of two items "
                                 "only, but found : "
                              << vec.size(),
                vec.size() == 2);

    // To be consistent with the $group stage, we consider "missing" to be equivalent to null when
    // grouping values into buckets.
    return key.missing() ? Value(BSONNULL) : std::move(key);
}

// TODO: Add logic here
void DocumentSourceCluster::addDocumentToBucket(const pair<Value, Document>& entry,
                                                   Bucket& bucket) {
    //invariant(pExpCtx->getValueComparator().evaluate(entry.first >= bucket._max));
    //bucket._max = entry.first;

    const size_t numAccumulators = _accumulatorFactories.size();
    _variables->setRoot(entry.second);
    for (size_t k = 0; k < numAccumulators; k++) {
        bucket._accums[k]->process(_expressions[k]->evaluate(_variables.get()), false);
    }
}

// TODO: Add logic here
void DocumentSourceCluster::populateBuckets() {
    invariant(_sorter);
    _sortedInput.reset(_sorter->done());
    _sorter.reset();

    // If there are no buckets, then we don't need to populate anything.
    //if (_nBuckets == 0) {
    //    return;
    //}

    // Calculate the approximate bucket size. We attempt to fill each bucket with this many
    // documents.
    /*long long approxBucketSize = std::round(double(_nDocuments) / double(_nBuckets));

    if (approxBucketSize < 1) {
        // If the number of buckets is larger than the number of documents, then we try to make as
        // many buckets as possible by placing each document in its own bucket.
        approxBucketSize = 1;
    }*/

    pair<Value, Document> currentValue;

    while (_sortedInput->more()) {
      currentValue = _sortedInput->next();
      
      //LOG(3) << "populateBuckets " << currentValue ;
      LOG(3) << "populateBuckets first:" << currentValue.first ;
      const vector<Value>& locationItem = currentValue.first.getArray();
      vector<Value>::const_iterator it = locationItem.begin();
      double docLongitude = it->getDouble();
      ++it;
      double docLatitude = it->getDouble();
      LOG(3) << "docLongitude " << docLongitude;
      LOG(3) << "docLatitude " << docLatitude;
      LOG(3) << "_lonDelta " << _lonDelta;
      LOG(3) << "_latDelta " << _latDelta;
      if (_buckets.empty()){
        LOG(3) << "_buckets.empty " ;
        Bucket currentBucket(
            pExpCtx, docLongitude, docLatitude, _accumulatorFactories);
        addDocumentToBucket(currentValue, currentBucket);
        addBucket(currentBucket);
      } else {
        LOG(3) << "_buckets.check " ;
        int isFound = 0;
        _bucketsIterator = _buckets.begin();
        Bucket& currentBucket = _buckets.front();
        while (_bucketsIterator != _buckets.end()) {
          LOG(3) << "currentBucket_Longitude " << docLongitude;
          LOG(3) << "currentBucket_Latitude " << docLatitude;
          LOG(3) << "abs Long" << abs(currentBucket._Longitude - docLongitude);
          LOG(3) << "abs _Latitude" << abs(currentBucket._Latitude - docLatitude);
          if(abs(currentBucket._Longitude - docLongitude) < _lonDelta) {
            if(abs(currentBucket._Latitude - docLatitude) < _latDelta) {
              isFound = 1;
              break;
            }
          }
          
          currentBucket = *(_bucketsIterator++);
          LOG(3) << "next ";
        }
        if(isFound == 1) {
          LOG(3) << "match ";
          addDocumentToBucket(currentValue, currentBucket);
        } else {
          LOG(3) << "no match ";
          Bucket newBucket(
            pExpCtx, docLongitude, docLatitude, _accumulatorFactories);
          addDocumentToBucket(currentValue, newBucket);
          addBucket(newBucket);
        }
      }

/*
      
      addDocumentToBucket(currentValue, currentBucket);*/
    }

}

DocumentSourceCluster::Bucket::Bucket(const boost::intrusive_ptr<ExpressionContext>& expCtx,
                                      double Longitude,
                                      double Latitude,
                                      vector<Accumulator::Factory> accumulatorFactories)
    : _Longitude(Longitude), _Latitude(Latitude) {
    _accums.reserve(accumulatorFactories.size());
    for (auto&& factory : accumulatorFactories) {
        _accums.push_back(factory(expCtx));
    }
}

void DocumentSourceCluster::addBucket(Bucket& newBucket) {
    _buckets.push_back(newBucket);
}

Document DocumentSourceCluster::makeDocument(const Bucket& bucket) {
    const size_t nAccumulatedFields = _fieldNames.size();
    MutableDocument out(1 + nAccumulatedFields);

    out.addField("_id", Value{Document{{"Longitude", bucket._Longitude}, {"Latitude", bucket._Latitude}}});

    const bool mergingOutput = false;
    for (size_t i = 0; i < nAccumulatedFields; i++) {
        Value val = bucket._accums[i]->getValue(mergingOutput);

        // To be consistent with the $group stage, we consider "missing" to be equivalent to null
        // when evaluating accumulators.
        out.addField(_fieldNames[i], val.missing() ? Value(BSONNULL) : std::move(val));
    }
    return out.freeze();
}

void DocumentSourceCluster::dispose() {
    _sortedInput.reset();
    _bucketsIterator = _buckets.end();
    pSource->dispose();
}

Value DocumentSourceCluster::serialize(bool explain) const {
    MutableDocument insides;

    insides["location"] = _locationExpression->serialize(explain);
    insides["lonDelta"] = Value(_lonDelta);
    insides["latDelta"] = Value(_latDelta);

    const size_t nOutputFields = _fieldNames.size();
    MutableDocument outputSpec(nOutputFields);
    for (size_t i = 0; i < nOutputFields; i++) {
        intrusive_ptr<Accumulator> accum = _accumulatorFactories[i](pExpCtx);
        outputSpec[_fieldNames[i]] =
            Value{Document{{accum->getOpName(), _expressions[i]->serialize(explain)}}};
    }
    insides["output"] = outputSpec.freezeToValue();

    return Value{Document{{getSourceName(), insides.freezeToValue()}}};
}


intrusive_ptr<DocumentSourceCluster> DocumentSourceCluster::create(
    const intrusive_ptr<ExpressionContext>& pExpCtx,
    const boost::intrusive_ptr<Expression>& locationExpression,
    Variables::Id numVariables,
    double lonDelta,
    double latDelta,
    std::vector<AccumulationStatement> accumulationStatements,
    uint64_t maxMemoryUsageBytes) {
    // If there is no output field specified, then add the default one.
    if (accumulationStatements.empty()) {
        accumulationStatements.emplace_back("count",
                                            AccumulationStatement::getFactory("$sum"),
                                            ExpressionConstant::create(pExpCtx, Value(1)));
    }
    return new DocumentSourceCluster(pExpCtx,
                                        locationExpression,
                                        numVariables,
                                        lonDelta,
                                        latDelta,
                                        accumulationStatements,
                                        maxMemoryUsageBytes);
}

DocumentSourceCluster::DocumentSourceCluster(
    const intrusive_ptr<ExpressionContext>& pExpCtx,
    const boost::intrusive_ptr<Expression>& locationExpression,
    Variables::Id numVariables,
    double lonDelta,
    double latDelta,
    std::vector<AccumulationStatement> accumulationStatements,
    uint64_t maxMemoryUsageBytes)
    : DocumentSource(pExpCtx),
      _lonDelta(lonDelta),
      _latDelta(latDelta),
      _maxMemoryUsageBytes(maxMemoryUsageBytes),
      _variables(stdx::make_unique<Variables>(numVariables)),
      _locationExpression(locationExpression) {

    invariant(!accumulationStatements.empty());
    for (auto&& accumulationStatement : accumulationStatements) {
        _fieldNames.push_back(std::move(accumulationStatement.fieldName));
        _accumulatorFactories.push_back(accumulationStatement.factory);
        _expressions.push_back(accumulationStatement.expression);
    }
}

namespace {

boost::intrusive_ptr<Expression> parselocationExpression(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    const BSONElement& locationField,
    const VariablesParseState& vps) {
    if (locationField.type() == BSONType::Object &&
        locationField.embeddedObject().firstElementFieldName()[0] == '$') {
        return Expression::parseObject(expCtx, locationField.embeddedObject(), vps);
    } else if (locationField.type() == BSONType::String &&
               locationField.valueStringData()[0] == '$') {
        return ExpressionFieldPath::parse(expCtx, locationField.str(), vps);
    } else {
        uasserted(
            40506,
            str::stream() << "The $cluster 'location' field must be defined as a $-prefixed "
                             "path or an expression object, but found: "
                          << locationField.toString(false, false));
    }
}
}  // namespace

intrusive_ptr<DocumentSource> DocumentSourceCluster::createFromBson(
    BSONElement elem, const intrusive_ptr<ExpressionContext>& pExpCtx) {
    uassert(40500,
            str::stream() << "Argument to $cluster stage must be an object, but found type: "
                          << typeName(elem.type())
                          << ".",
            elem.type() == BSONType::Object);

    VariablesIdGenerator idGenerator;
    VariablesParseState vps(&idGenerator);
    vector<AccumulationStatement> accumulationStatements;
    boost::intrusive_ptr<Expression> locationExpression;
    boost::optional<double> latDelta;
    boost::optional<double> lonDelta;

    for (auto&& argument : elem.Obj()) {
        const auto argName = argument.fieldNameStringData();
        if ("location" == argName) {
          locationExpression = parselocationExpression(pExpCtx, argument, vps);
        }  else if ("lonDelta" == argName) {
          Value argValue = Value(argument);
          uassert(
                40501,
                str::stream()
                    << "The $cluster 'lonDelta' field must be a numeric value, but found type: "
                    << typeName(argument.type()),
                argValue.numeric());
          lonDelta = argValue.coerceToDouble();
            
        } else if ("latDelta" == argName) {
          Value argValue = Value(argument);
          uassert(
                40502,
                str::stream()
                    << "The $cluster 'latDelta' field must be a numeric value, but found type: "
                    << typeName(argument.type()),
                argValue.numeric());
          latDelta = argValue.coerceToDouble();
            
        } else if ("output" == argName) {
            uassert(
                40503,
                str::stream() << "The $cluster 'output' field must be an object, but found type: "
                              << typeName(argument.type())
                              << ".",
                argument.type() == BSONType::Object);

            for (auto&& outputField : argument.embeddedObject()) {
                accumulationStatements.push_back(
                    AccumulationStatement::parseAccumulationStatement(pExpCtx, outputField, vps));
            }
        } else {
            uasserted(40504, str::stream() << "Unrecognized option to $cluster: " << argName << ".");
        }
    }
    
    uassert(40505,
            "$cluster requires 'location', 'lonDelta' and 'latDelta' to be specified",
            locationExpression && lonDelta && latDelta);

    return DocumentSourceCluster::create(pExpCtx,
                                            locationExpression,
                                            idGenerator.getIdCount(),
                                            lonDelta.get(),
                                            latDelta.get(),
                                            accumulationStatements);
}
}  // namespace mongo


#include "mongo/db/sorter/sorter.cpp"
// Explicit instantiation unneeded since we aren't exposing Sorter outside of this file.
