
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

 class Job891v0 extends GeneratableJob {

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
    

val map1 = source0.map(x=>x).withUdfComplexity(UDFComplexity.LINEAR)
                

 val source6 = planBuilder
 .readTextFile(datapath + "partsupp.tbl")
 .withName("Read PartSupplier")
 .map(PartSupplier.parseCsv)
 .map(PartSupplier.toTuple)
 .withName("Parse PartSupplier to tuple")
    

val filter5 = source6.filter(x=>x._3 <= 5003.0)
      

val sort4 = filter5.sort(_._1)
      

val filter3 = sort4.filter(x=>x._3 <= 7499.0)
      

val join2 = map1.keyBy[Long](_._2).join(filter3.keyBy[Long](_._1)).map(x => x.field1)
       

val map7 = join2.map(x=> {var count = 0; for (v <- x.productIterator) count+=1; x})
                  .withUdfComplexity(UDFComplexity.QUADRATIC)
                

 val source12 = planBuilder
 .readTextFile(datapath + "supplier.tbl")
 .withName("Read Supplier")
 .map(Supplier.parseCsv)
 .map(Supplier.toTuple)
 .withName("Parse Supplier to tuple")
    

val reduce10 = source12.reduce((x1, x2) => x1)
      

val map9 = reduce10.map(x=> {var count = 0; for (v1 <- x.productIterator; v2 <- x.productIterator) count+=1; x})
                  .withUdfComplexity(UDFComplexity.SUPERQUADRATIC)
       

val join8 = map7.keyBy[Long](_._2).join(map9.keyBy[Long](_._1)).map(x => x.field1)
       

val map13 = join8.map(x=>x).withUdfComplexity(UDFComplexity.LINEAR)
                

 val source18 = planBuilder
 .readTextFile(datapath + "nation.tbl")
 .withName("Read Nation")
 .map(Nation.parseCsv)
 .map(Nation.toTuple)
 .withName("Parse Nation to tuple")
    

val map17 = source18.map(x=> {var count = 0; for (v <- x.productIterator) count+=1; x})
                  .withUdfComplexity(UDFComplexity.QUADRATIC)
                

val map16 = map17.map(x=> {var count = 0; for (v1 <- x.productIterator; v2 <- x.productIterator) count+=1; x})
                  .withUdfComplexity(UDFComplexity.SUPERQUADRATIC)
       

val map15 = map16.map(x=>x).withUdfComplexity(UDFComplexity.LINEAR)
                

val join14 = map13.keyBy[Long](_._4).join(map15.keyBy[Long](_._1)).map(x => x.field1)
       

 return join14.collectNoExecute()
      

 }
}
  