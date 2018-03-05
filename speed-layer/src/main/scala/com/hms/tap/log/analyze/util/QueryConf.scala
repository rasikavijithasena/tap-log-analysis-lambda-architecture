package com.hms.tap.log.analyze.util

import com.typesafe.config.ConfigFactory

import scala.collection.JavaConversions._

class QueryConf extends Serializable{
  private var conf = ConfigFactory.parseResources("TypesafeConfig.conf")

  private val filterString = conf.getString("query.whereClause")
  private val groupingString = conf.getStringList("query.groupingFields").toList
  private val groupingFieldsAsString = conf.getString("query.groupingFieldsAsString")
  private val selectedFields = conf.getString("query.selectedFields")

  def getFilterString(): String ={
    filterString
  }

  def getGroupingFields(num : Int): String ={
    groupingString.apply(num)
  }

  def getGroupingFieldsAsArray(): Array[String] ={
    val groupingFieldsAsArray = groupingFieldsAsString.split(",")
    groupingFieldsAsArray
  }

  def getSelectedFieldsAsArray(): Array[String] ={
    selectedFields.split(",")
  }

}
