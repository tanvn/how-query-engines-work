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

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.BitVector
import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.VarCharVector
import io.andygrove.kquery.datatypes.ArrowFieldVector
import io.andygrove.kquery.datatypes.ArrowVectorBuilder
import io.andygrove.kquery.datatypes.ColumnVector
import io.andygrove.kquery.datatypes.RecordBatch
import io.andygrove.kquery.datatypes.Schema
import io.andygrove.kquery.physical.expressions.Expression

/** Execute a selection. */
class SelectionExec(val input: PhysicalPlan, val expr: Expression) : PhysicalPlan {

  override fun schema(): Schema {
    return input.schema()
  }

  override fun children(): List<PhysicalPlan> {
    return listOf(input)
  }

  override fun execute(): Sequence<RecordBatch> {
    val input = input.execute()
    return input.map { batch ->
      // result is a BitVector (0 and 1) evaluated from the expr on a RecordBatch (a RecordBatch is like a 2-dimension array)
      // a number of records, each record contains data of a group of fields (columns)
      val result = (expr.evaluate(batch) as ArrowFieldVector).field as BitVector
      val schema = batch.schema
      val columnCount = batch.schema.fields.size // number of columns
      // iterate over each column -> get a ColumnVector for that column
      // from the ColumnVector and the result (BitVector) -> create a FieldVector containing data matching the filter (bit value = 1)
      // filteredFields is a List<FieldVector> where each FieldVector has the same size as in filter, the same result (BitVector) is passed)
      val filteredFields = (0 until columnCount).map { filter(batch.field(it), result) }
      val fields = filteredFields.map { ArrowFieldVector(it) }
      RecordBatch(schema, fields)
    }
  }

  private fun filter(v: ColumnVector, selection: BitVector): FieldVector {
    val filteredVector = VarCharVector("v", RootAllocator(Long.MAX_VALUE))
    filteredVector.allocateNew()

    val builder = ArrowVectorBuilder(filteredVector)

    var count = 0
    (0 until selection.valueCount).forEach {
      if (selection.get(it) == 1) {
        builder.set(count, v.getValue(it))
        count++
      }
    }
    filteredVector.valueCount = count
    return filteredVector
  }
}
