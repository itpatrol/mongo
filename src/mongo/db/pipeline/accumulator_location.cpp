/**
 * Copyright (c) 2011 10gen Inc.
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
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "mongo/db/pipeline/accumulator.h"

#include "mongo/db/pipeline/accumulation_statement.h"

#include "mongo/db/pipeline/expression.h"
#include "mongo/db/pipeline/expression_context.h"
#include "mongo/db/pipeline/value.h"
#include "mongo/util/log.h"


namespace mongo {

using boost::intrusive_ptr;
using std::vector;

REGISTER_ACCUMULATOR(location, AccumulatorLocation::create);
REGISTER_EXPRESSION(location, ExpressionLocation::parse);

const char* AccumulatorLocation::getOpName() const {
    return "$location";
}


void AccumulatorLocation::processInternal(const Value& input, bool merging) {
  LOG(3) << "AccumulatorLocation processInternal" ;
    if (merging) {
      LOG(3) << "AccumulatorLocation processInternal:mergin" ;
        // If we're merging, we need to take apart the arrays we
        // receive and put their elements into the array we are collecting.
        // If we didn't, then we'd get an array of arrays, with one array
        // from each merge source.
        verify(input.getType() == Array);

        const vector<Value>& vec = input.getArray();
        vpValue.insert(vpValue.end(), vec.begin(), vec.end());

        for (size_t i = 0; i < vec.size(); i++) {
            _memUsageBytes += vec[i].getApproximateSize();
        }
        return;
    }
    

    if (!input.missing()) {
      LOG(3) << "AccumulatorLocation save" ;
      _current = input;
      size_t vpValueIdx = 0;
      const vector<Value>& vec = input.getArray();
      LOG(3) << "AccumulatorLocation vpValue size "  << vpValue.size();
      //LOG(3) << "AccumulatorLocation vpValue vpValue "  << vpValue; 
      LOG(3) << "AccumulatorLocation processInternal input " << input;
      LOG(3) << "AccumulatorLocation processInternal inputType " <<input.getType();
      LOG(3) << "AccumulatorLocation processInternal vec " << vec.size();
      while (vpValueIdx < vpValue.size()) {
        LOG(3) << "AccumulatorLocation processInternal while "  << vpValueIdx;
        const Value& cutItem = vpValue[vpValueIdx];
        LOG(3) << "AccumulatorLocation processInternal curItem " << cutItem;
        
        const vector<Value>& locationItem = cutItem.getArray();
        LOG(3) << "AccumulatorLocation processInternal locationItem " << locationItem.size();

        if(abs(locationItem.end() - vec.end()) < 0.6588013
          && abs(locationItem.begin() - vec.begin()) < 0.6588013) {
            LOG(3) << "AccumulatorLocation processInternal match" ;
            _current = vpValue[vpValueIdx];
            return;
          }
        ++vpValueIdx;
      }
      LOG(3) << "AccumulatorLocation processInternal add to set" ;
      vpValue.push_back(input);
      _memUsageBytes += input.getApproximateSize();
    }
}


Value AccumulatorLocation::getValue(bool toBeMerged) const {
  LOG(3) << "AccumulatorLocation getValue" ;
    if (toBeMerged) {
      LOG(3) << "AccumulatorLocation getValue merged" ;
      return Value(vpValue);
    }

    return Value(_current);
}

AccumulatorLocation::AccumulatorLocation(const boost::intrusive_ptr<ExpressionContext>& expCtx)
    : Accumulator(expCtx) {
    // This is a fixed size Accumulator so we never need to update this
    _memUsageBytes = sizeof(*this);
}

void AccumulatorLocation::reset() {
  LOG(3) << "AccumulatorLocation reset" ;
    _current = Value();
    vector<Value>().swap(vpValue);
    _memUsageBytes = sizeof(*this);
}

intrusive_ptr<Accumulator> AccumulatorLocation::create(
    const boost::intrusive_ptr<ExpressionContext>& expCtx) {
    return new AccumulatorLocation(expCtx);
}
}
