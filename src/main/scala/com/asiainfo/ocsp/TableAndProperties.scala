package com.asiainfo.ocsp

import org.apache.flink.table.api.Table

import scala.collection.mutable

class TableAndProperties(val table: Table, val props: mutable.LinkedHashMap[String, String]) {
}
