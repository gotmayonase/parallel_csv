#! /usr/bin/ruby
require './parallel_csv'
ParallelCSV.for_each("/Users/mmayo/Downloads/GameNews/Gifts-Table.csv", threads: ARGV[0] || 1, headers: true) { |r| sleep 0.001; print '.' }