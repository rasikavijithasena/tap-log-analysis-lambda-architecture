package com.hms.tap.log.analyze.util

import com.typesafe.config.ConfigFactory

class OutputConf extends Serializable {
  private val conf = ConfigFactory.parseResources("TypesafeConfig.conf")

  private val _batchFormat = conf.getString("batch.outputFormat")
  private val _batchOutputPath = conf.getString("batch.path")

  private val _structuredFormat = conf.getString("structured.format")
  private val _structuredPath = conf.getString("structured.parquet.parquetPath")
  private val _structuredCheckpointPath = conf.getString("structured.parquet.checkpointPath")

  def batchFormat = _batchFormat

  def batchOutputPath = _batchOutputPath

  def structuredFormat = _structuredFormat

  def structuredPath = _structuredPath

  def structuredCheckpointPath = _structuredCheckpointPath


}
