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

import java.io.Serializable;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.types.DataTypes;
import org.giscience.utils.geogrid.grids.ISEA3H;

public class Isea3hUDF implements UDF3<Integer, Double, Double, Long>, Serializable {
  public static void register(SparkSession spark, String name) {
    spark.udf().register(name, new Isea3hUDF(), DataTypes.LongType);
  }

  @Override
  public Long call(Integer resolution, Double lat, Double lng) {
    try {
      ISEA3H grid = new ISEA3H(resolution);
      return grid.cellForLocation(lat, lng).getID();
    } catch (Exception e) {
      System.err.println(e);
      return null;
    }
  }
}
