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
package org.apache.pig.backend.hadoop.executionengine.spark.converter;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.backend.hadoop.executionengine.spark.operator.POCollectedGroupSpark;
import org.apache.pig.data.Tuple;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.rdd.RDD;


@SuppressWarnings({"serial"})
public class CollectedGroupConverter implements POConverter<Tuple, Tuple, POCollectedGroupSpark> {

    @Override
    public RDD<Tuple> convert(List<RDD<Tuple>> predecessors,
                              POCollectedGroupSpark physicalOperator) throws IOException {
        SparkUtil.assertPredecessorSize(predecessors, physicalOperator, 1);
        RDD<Tuple> rdd = predecessors.get(0);
        RDD<Tuple> rdd2 = rdd.coalesce(1, false, null);
        // performing count should better be avoided
        long count = rdd2.count();;
        CollectedGroupFunction collectedGroupFunction
                = new CollectedGroupFunction(physicalOperator, count);
        return rdd.toJavaRDD().mapPartitions(collectedGroupFunction, true).rdd();
    }

    private static class CollectedGroupFunction implements FlatMapFunction<Iterator<Tuple>, Tuple> {

        private POCollectedGroupSpark poCollectedGroup;

        public long total_limit;
        public long current_val;
        public boolean proceed;

        private CollectedGroupFunction(POCollectedGroupSpark poCollectedGroup, long count) {
            this.poCollectedGroup = poCollectedGroup;
            this.total_limit = count;
            this.current_val = 0;
        }

        public Iterable<Tuple> call(final Iterator<Tuple> input) {

            return new Iterable<Tuple>() {

                @Override
                public Iterator<Tuple> iterator() {
                    return new POOutputConsumerIterator(input) {
                        protected void attach(Tuple tuple) {
                            poCollectedGroup.setInputs(null);
                            poCollectedGroup.attachInput(tuple);
                            poCollectedGroup.setParentPlan(poCollectedGroup.getPlans().get(0));

                            try {
                                current_val = current_val + 1;
                                if (current_val == total_limit) {
                                    proceed = true;
                                } else {
                                    proceed = false;
                                }
                            } catch (Exception e) {
                                System.out.println("Error in CollectedGroupConverter :" + e);
                                e.printStackTrace();
                            }
                        }

                        protected Result getNextResult() throws ExecException {
                            return poCollectedGroup.getNextTuple(proceed);
                        }
                    };
                }
            };
        }
    }
}