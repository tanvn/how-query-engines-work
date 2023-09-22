// Copyright 2020 Andy Grove
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.andygrove.kquery.physical

import io.andygrove.kquery.datatypes.*
import io.andygrove.kquery.physical.expressions.Accumulator
import io.andygrove.kquery.physical.expressions.AggregateExpression
import io.andygrove.kquery.physical.expressions.Expression
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot

class HashAggregateExec(
    val input: PhysicalPlan,
    val groupExpr: List<Expression>,
    val aggregateExpr: List<AggregateExpression>,
    val schema: Schema
) : PhysicalPlan {

    override fun schema(): Schema {
        return schema
    }

    override fun children(): List<PhysicalPlan> {
        return listOf(input)
    }

    override fun toString(): String {
        return "HashAggregateExec: groupExpr=$groupExpr, aggrExpr=$aggregateExpr"
    }

    override fun execute(): Sequence<RecordBatch> {

        val map = HashMap<List<Any?>, List<Accumulator>>()

        // for each batch from the input executor
        input.execute().iterator().forEach { batch ->

            // evaluate the grouping expressions
            // groupExpr: (for example) GROUP BY col1, col2 -> List<Expression> with size = 2
            // groupKeys is a List of 2 ColumnVectors, one for col1 and  one for col2
            val groupKeys: List<ColumnVector> = groupExpr.map { it.evaluate(batch) }

            // evaluate the expressions that are inputs to the aggregate functions
            // AggregateExpression might be, for example: MAX(col3) -> col3 is the input expression
            // then inputExpression().evaluate(batch) returns a ColumnVector on col3: a vector of data for col3
            val aggrInputValues: List<ColumnVector> = aggregateExpr.map { it.inputExpression().evaluate(batch) }

            // for each row in the batch
            (0 until batch.rowCount()).forEach { rowIndex ->

                // create the key for the hash map
                val rowKey: List<Any?> =
                    groupKeys.map {
                        val value = it.getValue(rowIndex)
                        when (value) {
                            is ByteArray -> String(value)
                            else -> value
                        }
                    }

                // for example GROUP BY col1, col2 -> rowKey for rowIndex=0 can be List(1,2), rowIndex=1 can be List(2,4) ...
                // println(rowKey)

                // get or create accumulators for this grouping key
                val accumulators: List<Accumulator> =
                    map.getOrPut(rowKey) { aggregateExpr.map { it.createAccumulator() } }
                // accumulators and aggrInputValues have a same size


                // perform accumulation
                accumulators.withIndex().forEach { accum ->
                    // accum at index 0 is an Accumulator on a ColumnVector at index 0 of aggrInputValues and so on
                    val value =
                        aggrInputValues[accum.index].getValue(rowIndex) // value of col3 at rowIndex in the VectorColumn
                    accum.value.accumulate(value) // accumulate Max
                }
            }
        }

        // create result batch containing final aggregate values
        val root = VectorSchemaRoot.create(schema.toArrow(), RootAllocator(Long.MAX_VALUE))
        root.allocateNew()
        root.rowCount = map.size

        val builders: List<ArrowVectorBuilder> = root.fieldVectors.map { ArrowVectorBuilder(it) }

        // map could be something like: (For (SELECT MAX(col3) GROUP BY col1, col2))
        // key: List(1,2), value: List(Accumulator(finalValue: 5))
        // key: List(1,3), value: List(Accumulator(finalValue: 6))
        map.entries.withIndex().forEach { entry ->
            val rowIndex = entry.index // 0
            val groupingKey: List<Any?> = entry.value.key // List(1,2)
            val accumulators: List<Accumulator> =
                entry.value.value //List(Accumulator(finalValue: 5))
            // with rowIndex=0
            // builders[0]: ArrowVectorBuilder (for col1) -> set(0,1)
            // builders[1]: ArrowVectorBuilder (for col2) -> set(0,2)

            // with rowIndex=1
            // builders[0]: ArrowVectorBuilder (for col1) -> set(1,1)
            // builders[1]: ArrowVectorBuilder (for col2) -> set(1,3)
            groupExpr.indices.forEach {
                builders[it].set(
                    rowIndex,
                    groupingKey[it]
                )
            }
            // with rowIndex=0
            // builders[2]: ArrowVectorBuilder (for max(col3)) -> set(0,5)

            // with rowIndex=1
            // builders[2]: ArrowVectorBuilder (for max(col3)) -> set(1,6)
            aggregateExpr.indices.forEach {
                builders[groupExpr.size + it].set(rowIndex, accumulators[it].finalValue())
            }

            // builders[0] => VectorColumn: [1, 1] (of col1)
            // builder[1] =>  VectorColumn: [2, 3] (of col2)
            // builder[2] => VectorColumn: [5, 6] (of Max(col3)

        }

        val outputBatch = RecordBatch(schema, root.fieldVectors.map { ArrowFieldVector(it) })
        // println("HashAggregateExec output:\n${outputBatch.toCSV()}")
        return listOf(outputBatch).asSequence()
    }
}
