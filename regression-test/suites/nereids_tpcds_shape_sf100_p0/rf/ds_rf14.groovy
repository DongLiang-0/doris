/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

suite("ds_rf14") {
    String db = context.config.getDbNameByFile(new File(context.file.parent))
    sql "use ${db}"
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'
    sql 'set exec_mem_limit=21G'
    sql 'set be_number_for_test=3'
    sql 'set parallel_fragment_exec_instance_num=8; '
    sql 'set parallel_pipeline_task_num=8; '
    sql 'set forbid_unknown_col_stats=true'
    sql 'set broadcast_row_count_limit = 30000000'
    sql 'set enable_nereids_timeout = false'
    sql 'set enable_pipeline_engine=true'
    sql 'set enable_runtime_filter_prune=false'
    sql 'set expand_runtime_filter_by_inner_join=false'
    String stmt = '''
    explain physical plan
    with  cross_items as
 (select i_item_sk ss_item_sk
 from item,
 (select iss.i_brand_id brand_id
     ,iss.i_class_id class_id
     ,iss.i_category_id category_id
 from store_sales
     ,item iss
     ,date_dim d1
 where ss_item_sk = iss.i_item_sk
   and ss_sold_date_sk = d1.d_date_sk
   and d1.d_year between 2000 AND 2000 + 2
 intersect 
 select ics.i_brand_id
     ,ics.i_class_id
     ,ics.i_category_id
 from catalog_sales
     ,item ics
     ,date_dim d2
 where cs_item_sk = ics.i_item_sk
   and cs_sold_date_sk = d2.d_date_sk
   and d2.d_year between 2000 AND 2000 + 2
 intersect
 select iws.i_brand_id
     ,iws.i_class_id
     ,iws.i_category_id
 from web_sales
     ,item iws
     ,date_dim d3
 where ws_item_sk = iws.i_item_sk
   and ws_sold_date_sk = d3.d_date_sk
   and d3.d_year between 2000 AND 2000 + 2)
 t where i_brand_id = brand_id
      and i_class_id = class_id
      and i_category_id = category_id
),
 avg_sales as
 (select avg(quantity*list_price) average_sales
  from (select ss_quantity quantity
             ,ss_list_price list_price
       from store_sales
           ,date_dim
       where ss_sold_date_sk = d_date_sk
         and d_year between 2000 and 2000 + 2
       union all 
       select cs_quantity quantity 
             ,cs_list_price list_price
       from catalog_sales
           ,date_dim
       where cs_sold_date_sk = d_date_sk
         and d_year between 2000 and 2000 + 2 
       union all
       select ws_quantity quantity
             ,ws_list_price list_price
       from web_sales
           ,date_dim
       where ws_sold_date_sk = d_date_sk
         and d_year between 2000 and 2000 + 2) x)
  select  channel, i_brand_id,i_class_id,i_category_id,sum(sales), sum(number_sales)
 from(
       select 'store' channel, i_brand_id,i_class_id
             ,i_category_id,sum(ss_quantity*ss_list_price) sales
             , count(*) number_sales
       from store_sales
           ,item
           ,date_dim
       where ss_item_sk in (select ss_item_sk from cross_items)
         and ss_item_sk = i_item_sk
         and ss_sold_date_sk = d_date_sk
         and d_year = 2000+2 
         and d_moy = 11
       group by i_brand_id,i_class_id,i_category_id
       having sum(ss_quantity*ss_list_price) > (select average_sales from avg_sales)
       union all
       select 'catalog' channel, i_brand_id,i_class_id,i_category_id, sum(cs_quantity*cs_list_price) sales, count(*) number_sales
       from catalog_sales
           ,item
           ,date_dim
       where cs_item_sk in (select ss_item_sk from cross_items)
         and cs_item_sk = i_item_sk
         and cs_sold_date_sk = d_date_sk
         and d_year = 2000+2 
         and d_moy = 11
       group by i_brand_id,i_class_id,i_category_id
       having sum(cs_quantity*cs_list_price) > (select average_sales from avg_sales)
       union all
       select 'web' channel, i_brand_id,i_class_id,i_category_id, sum(ws_quantity*ws_list_price) sales , count(*) number_sales
       from web_sales
           ,item
           ,date_dim
       where ws_item_sk in (select ss_item_sk from cross_items)
         and ws_item_sk = i_item_sk
         and ws_sold_date_sk = d_date_sk
         and d_year = 2000+2
         and d_moy = 11
       group by i_brand_id,i_class_id,i_category_id
       having sum(ws_quantity*ws_list_price) > (select average_sales from avg_sales)
 ) y
 group by rollup (channel, i_brand_id,i_class_id,i_category_id)
 order by channel,i_brand_id,i_class_id,i_category_id
 limit 100;

    '''
    String plan = sql "${stmt}"
    log.info(plan)
    def getRuntimeFilters = { plantree ->
        {
            def lst = []
            plantree.eachMatch("RF\\d+\\[[^#]+#\\d+->\\[[^\\]]+\\]") {
                ch ->
                    {
                        lst.add(ch.replaceAll("#\\d+", ''))
                    }
            }
            return lst.join(',')
        }
    }
    
    // def outFile = "regression-test/suites/nereids_tpcds_shape_sf100_p0/ddl/rf/rf.14"
    // File file = new File(outFile)
    // file.write(getRuntimeFilters(plan))
    
     assertEquals("RF6[class_id->[i_class_id],RF7[category_id->[i_category_id],RF8[brand_id->[i_brand_id],RF1[i_item_sk->[ss_item_sk],RF0[d_date_sk->[ss_sold_date_sk],RF3[i_item_sk->[cs_item_sk],RF2[d_date_sk->[cs_sold_date_sk],RF5[i_item_sk->[ws_item_sk],RF4[d_date_sk->[ws_sold_date_sk],RF9[d_date_sk->[ss_sold_date_sk],RF10[d_date_sk->[cs_sold_date_sk],RF11[d_date_sk->[ws_sold_date_sk],RF14[ss_item_sk->[ss_item_sk],RF13[i_item_sk->[ss_item_sk],RF12[d_date_sk->[ss_sold_date_sk],RF17[cs_item_sk->[ss_item_sk],RF16[i_item_sk->[cs_item_sk],RF15[d_date_sk->[cs_sold_date_sk],RF20[ws_item_sk->[ss_item_sk],RF19[i_item_sk->[ws_item_sk],RF18[d_date_sk->[ws_sold_date_sk]", getRuntimeFilters(plan))
}
