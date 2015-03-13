/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.backend.hadoop.executionengine.spark.operator;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ExpressionOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POCollectedGroup;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.InternalCachedBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.OperatorKey;

import java.util.ArrayList;
import java.util.List;

/**
 * Converter for performing collected group
 */
public class POCollectedGroupSpark extends POCollectedGroup {

    public POCollectedGroupSpark(POCollectedGroup copy) {
        super(copy);
    }

    public Result getNextTuple(boolean proceed) throws ExecException {
        Result inp = null;
        Result res = null;

        while (true) {
            inp = processInput();
            if (inp.returnStatus == POStatus.STATUS_EOP ||
                    inp.returnStatus == POStatus.STATUS_ERR) {
                break;
            }

            if (inp.returnStatus == POStatus.STATUS_NULL) {
                continue;
            }

            for (PhysicalPlan ep : plans) {
                ep.attachInput((Tuple) inp.result);
            }

            List<Result> resLst = new ArrayList<Result>();
            for (ExpressionOperator op : leafOps) {
                res = op.getNext(op.getResultType());
                if (res.returnStatus != POStatus.STATUS_OK) {
                    return new Result();
                }
                resLst.add(res);
            }

            Tuple tup = constructOutput(resLst, (Tuple) inp.result);
            Object curKey = tup.get(0);

            // the first time, just create a new buffer and continue.
            if (prevKey == null && outputBag == null) {

                if (PigMapReduce.sJobConfInternal.get() != null) {
                    String bagType = PigMapReduce.sJobConfInternal.get().get("pig.cachedbag.type");
                    if (bagType != null && bagType.equalsIgnoreCase("default")) {
                        useDefaultBag = true;
                    }
                }
                prevKey = curKey;
                outputBag = useDefaultBag ? BagFactory.getInstance().newDefaultBag()
                        // In a very rare case if there is a POStream after this
                        // POCollectedGroup in the pipeline and is also blocking the pipeline;
                        // constructor argument should be 2. But for one obscure
                        // case we don't want to pay the penalty all the time.

                        // Additionally, if there is a merge join(on a different key) following POCollectedGroup
                        // default bags should be used. But since we don't allow anything
                        // before Merge Join currently we are good.
                        : new InternalCachedBag(1);
                outputBag.add((Tuple) tup.get(1));
                continue;
            }

            // no key change
            if (prevKey == null && curKey == null) {
                outputBag.add((Tuple) tup.get(1));
                continue;
            }

            // no key change
            if (prevKey != null && curKey != null && ((Comparable) curKey).compareTo(prevKey) == 0) {
                outputBag.add((Tuple) tup.get(1));
                continue;
            }

            // key change
            Tuple tup2 = mTupleFactory.newTuple(2);
            tup2.set(0, prevKey);
            tup2.set(1, outputBag);
            res.result = tup2;

            prevKey = curKey;
            outputBag = useDefaultBag ? BagFactory.getInstance().newDefaultBag()
                    : new InternalCachedBag(1);
            outputBag.add((Tuple) tup.get(1));
            return res;
        }

        // Since the output is buffered, we need to flush the last
        // set of records when the close method is called by mapper.
        if (this.parentPlan.endOfAllInput || proceed) {
            if (outputBag != null) {
                Tuple tup = mTupleFactory.newTuple(2);
                tup.set(0, prevKey);
                tup.set(1, outputBag);
                outputBag = null;
                return new Result(POStatus.STATUS_OK, tup);
            }

            return new Result(POStatus.STATUS_EOP, null);
        }

        return inp;
    }
}
