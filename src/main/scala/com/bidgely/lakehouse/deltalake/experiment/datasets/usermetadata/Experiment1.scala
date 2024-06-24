package com.bidgely.lakehouse.deltalake.experiment.datasets.usermetadata


object Experiment1 {
  /**
   * Describe
   * @param args
   */
  def main(args: Array[String]) = {

    write
    deltaWrite
    clean
    clustering yes
      precombine no
  }
}