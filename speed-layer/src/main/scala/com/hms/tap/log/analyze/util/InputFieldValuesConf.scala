package com.hms.tap.log.analyze.util

import com.typesafe.config.ConfigFactory

import scala.collection.JavaConversions._

class InputFieldValuesConf extends Serializable{

  private val conf = ConfigFactory.parseResources("TypesafeConfig.conf")

  private val selectedFields = conf.getStringList("input.selectedFields").toList
  private val delimeter = conf.getString("input.delimeter")
  private val fieldCount = conf.getInt("input.fieldCount")
  private val directory = conf.getString("input.directory")
  private val schemaString = conf.getString("input.schemaString")
  private val schemaSeperator = conf.getString("input.fieldSeperator")
  private val allFields = conf.getString("input.fields")

  def getSelectedFields(): List[String] =  {
    selectedFields
  }

  def getNumberOfSelectedFields(): Int ={
    selectedFields.size
  }

  def getFieldNumber(listNumber: Int): Int ={
    val field = selectedFields.get(listNumber)
    val fieldNumber = conf.getInt("input."+field+".fieldNumber")
    fieldNumber
  }

  def getDelimeter(): String ={
    delimeter
  }

  def getFieldSeperator(): String ={
    schemaSeperator
  }

  def getFieldCount(): Int ={
    fieldCount
  }

  def getDirectory(): String ={
    directory
  }

  def getSchema(): String={
    schemaString
  }


  def getSubString(listNumber: Int): List[Integer]={
    val field = selectedFields.get(listNumber)
    val substring = conf.getBoolean("input."+field+".substring")

    var substringValue: List[Integer] = null

    if(substring == true) {
      substringValue = conf.getIntList("input."+field+".substringValue").toList

    } else {

    }
    substringValue
  }

  def getFieldsAsArray(): Array[String] ={
    val fields = "timestamp," + allFields
    val fieldArray = fields.split(",")
    fieldArray
  }

}
