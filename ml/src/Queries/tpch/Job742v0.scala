
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

 package org.apache.wayang.ml.generatables

 import org.apache.wayang.api._
 import org.apache.wayang.core.api.{Configuration, WayangContext}
 import org.apache.wayang.core.plugin.Plugin
 import org.apache.wayang.apps.tpch.data.{Customer, Order, LineItem, Nation, Part, PartSupplier, Supplier}
 import org.apache.wayang.core.plugin.Plugin
 import java.sql.Date
 import org.apache.wayang.apps.util.{ExperimentDescriptor, Parameters, ProfileDBHelper}
 import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
 import org.apache.wayang.apps.tpch.CsvUtils
 import org.apache.wayang.ml.training.GeneratableJob
 import org.apache.wayang.core.function.UDFComplexity

 class Job742v0 extends GeneratableJob {

def buildPlan(args: Array[String]): DataQuanta[_] = {

 val dateFormatter: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")
 val plugins = Parameters.loadPlugins(args(0))
 val datapath = args(1)

 implicit val configuration = new Configuration
 val wayangCtx = new WayangContext(configuration)
 plugins.foreach(wayangCtx.register)
 val planBuilder = new PlanBuilder(wayangCtx)
 .withJobName(s"TPC-H (${this.getClass.getSimpleName})")

   
 val source0 = planBuilder
 .readTextFile(datapath + "lineitem.tbl")
 .withName("Read LineItem")
 .map(LineItem.parseCsv)
 .map(LineItem.toTuple)
 .withName("Parse LineItem to tuple")
    

val filter1 = source0.filter(x=>x._11 <= CsvUtils.parseDate("1995-06-19"))
      

 val source6 = planBuilder
 .readTextFile(datapath + "partsupp.tbl")
 .withName("Read PartSupplier")
 .map(PartSupplier.parseCsv)
 .map(PartSupplier.toTuple)
 .withName("Parse PartSupplier to tuple")
    

val map5 = source6.map(x=>x).withUdfComplexity(UDFComplexity.LINEAR)
                

val map4 = map5.map(x=> {var count = 0; for (v <- x.productIterator) count+=1; x})
                  .withUdfComplexity(UDFComplexity.QUADRATIC)
                

val group3 = map4.reduceByKey(_._1, (t1,t2) => t1)
          

val join2 = filter1.keyBy[Long](_._2).join(group3.keyBy[Long](_._1)).map(x => x.field1)
       

val map7 = join2.map(x=> {var count = 0; for (v1 <- x.productIterator; v2 <- x.productIterator) count+=1; x})
                  .withUdfComplexity(UDFComplexity.SUPERQUADRATIC)
       

 val source12 = planBuilder
 .readTextFile(datapath + "supplier.tbl")
 .withName("Read Supplier")
 .map(Supplier.parseCsv)
 .map(Supplier.toTuple)
 .withName("Parse Supplier to tuple")
    

val map11 = source12.map(x=>x).withUdfComplexity(UDFComplexity.LINEAR)
                

val group10 = map11.reduceByKey(_._3, (t1,t2) => t1)
          

val map9 = group10.map(x=> {var count = 0; for (v <- x.productIterator) count+=1; x})
                  .withUdfComplexity(UDFComplexity.QUADRATIC)
                

val join8 = map7.keyBy[Long](_._2).join(map9.keyBy[Long](_._1)).map(x => x.field1)
       

 val source17 = planBuilder
 .readTextFile(datapath + "nation.tbl")
 .withName("Read Nation")
 .map(Nation.parseCsv)
 .map(Nation.toTuple)
 .withName("Parse Nation to tuple")
    

val map16 = source17.map(x=> {var count = 0; for (v1 <- x.productIterator; v2 <- x.productIterator) count+=1; x})
                  .withUdfComplexity(UDFComplexity.SUPERQUADRATIC)
       

val group15 = map16.reduceByKey(_._2, (t1,t2) => t1)
          

val map14 = group15.map(x=>x).withUdfComplexity(UDFComplexity.LINEAR)
                

val join13 = join8.keyBy[Long](_._4).join(map14.keyBy[Long](_._1)).map(x => x.field1)
       

val map18 = join13.map(x=> {var count = 0; for (v <- x.productIterator) count+=1; x})
                  .withUdfComplexity(UDFComplexity.QUADRATIC)
                

 return map18.collectNoExecute()
      

 }
}
  