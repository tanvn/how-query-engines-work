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

package io.andygrove.kquery.optimizer

import io.andygrove.kquery.logical.*

class ProjectionPushDownRule : OptimizerRule {

    override fun optimize(plan: LogicalPlan): LogicalPlan {
        return pushDown(plan, mutableSetOf())
    }

    private fun pushDown(plan: LogicalPlan, columnNames: MutableSet<String>): LogicalPlan {

        return when (plan) {
            // SELECT col1, col2 FROM tbl1
            is Projection -> {
                extractColumns(plan.expr, plan.input, columnNames)
                val input = pushDown(plan.input, columnNames)
                Projection(input, plan.expr)
            }

            // SELECT col3 FROM tbl WHERE col1 = 'A' AND col2 > 0
            // plan.exp: WHERE col1 = 'A' AND col2 > 0 -> And(l: LogicalExpr, r: LogicalExpr)
            // where:
            //      l: Eq(l: LogicalExpr, r: LogicalExpr) -> l: col1 , r: 'A'
            //      r: Lt(l: LogicalExpr, r: LogicalExpr): l: col2, r: 0
            is Selection -> {
                extractColumns(plan.expr, plan.input, columnNames) // col1, col2
                // for example, the plan.input can be a Scan, then the returned input here will be a Scan
                // (no additional recursive call when the plan is a Scan)
                val input: LogicalPlan =
                    pushDown(plan.input, columnNames) // recursive here as Selection has a child logical plan

                Selection(input, plan.expr)
            }

            // SELECT MAX(col3) FROM tbl1 GROUP BY col1, col2
            is Aggregate -> {
                extractColumns(plan.groupExpr, plan.input, columnNames) // col1, col2
                extractColumns(plan.aggregateExpr.map { it.expr }, plan.input, columnNames) // col3
                val input =
                    pushDown(plan.input, columnNames) // recursive here as Aggregate has a child logical plan (input)
                Aggregate(input, plan.groupExpr, plan.aggregateExpr)
            }

            is Scan -> {
                val validFieldNames =
                    plan.dataSource.schema().fields.map { it.name }.toSet() // get all columns of the datasource
                val pushDown: List<String> =
                    validFieldNames.filter { columnNames.contains(it) }.toSet().sorted() // only get necessary columns
                Scan(plan.path, plan.dataSource, pushDown)
            }

            else -> throw IllegalStateException("ProjectionPushDownRule does not support plan: $plan")
        }
    }
}
