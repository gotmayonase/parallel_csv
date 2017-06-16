require 'fileutils'
require 'csv'
class ParallelCSV

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
      puts "Line count: #{processor.line_count}"
      current_line = @header_line ? 2 : 1
      @threads = []
      while current_line < processor.line_count
        @threads << Thread.new(current_line, processor.per_thread) do |line, inc_amount|
          csv_string = `sed -n #{line},#{line + inc_amount}p #{file_path}`.strip
          CSV.parse(csv_string, headers: processor.header_line || false) do |row|
            block.call(row)
          end
          puts "Finished processing lines #{line} - #{line + inc_amount}"
        end
        current_line += processor.per_thread
      end
      @threads.each { |thread| thread.join }
    end
  end

end
