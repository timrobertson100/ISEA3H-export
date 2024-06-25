/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class TestExport {

  public static void main(String[] args) {
    SparkSession spark =
        SparkSession.builder().appName("ISEA3H Export").enableHiveSupport().getOrCreate();
    spark.sql("use tim");

    Isea3hUDF.register(spark, "toCellID");

    Dataset<Row> r =
        spark.sql(
            String.format(
                "        SELECT "
                    + "    toCellID(6, lat, lng) AS cellId, "
                    + "    family, "
                    + "    basisOfRecord, "
                    + "    sum(occCount) AS occCount "
                    + "  FROM o "
                    + "  GROUP BY toCellID(6, lat, lng), family, basisOfRecord"));
    r.rdd().saveAsTextFile("/tmp/tim.csv");
  }
}
