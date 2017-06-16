require 'fileutils'
require 'csv'

class ParallelCsv

  attr_accessor :threads, :header_line, :opts, :line_count, :per_thread

  def initialize(file_path, opts = {})
    @threads = opts.delete(:threads) || 2
    headers = opts.delete(:headers)
    if headers
      @header_line = `head -1 #{file_path}`
    end
    @opts = opts
    @line_count = `wc -l #{file_path}`.strip.split(' ')[0].to_i
    @per_thread = (@line_count / @threads.to_f).ceil
  end

  class << self
    def for_each(file_path, opts = {}, &block)
      processor = new(file_path, opts)
      current_line = @header_line ? 2 : 1
      while current_line < processor.line_count
        fork do
          csv_string = `sed -n #{current_line},#{current_line + processor.per_thread}p #{file_path}`.strip
          CSV.parse(csv_string, opts.merge(headers: processor.header_line || false)) do |row|
            block.call(row)
          end
        end
        Process.wait
      end
      current_line += processor.per_thread
    end
  end

end
