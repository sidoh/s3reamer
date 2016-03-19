require 'set'
require 'logger'
require 'rb-inotify'
require 'aws-sdk'
require 'thread/pool'
require 'timeout'

require_relative 's3_write_stream'

module S3reamer
  class DirectoryStreamer
    def initialize(dir, bucket)
      @dir = dir
      @bucket = bucket
      @log = Logger.new(STDOUT)

      @ignored_files = Set.new
      @dir_watch = INotify::Notifier.new
      @pool = Thread.pool(4)

      @dir_watch.watch(@dir, :open, :recursive) do |e|
        filename = e.absolute_name
        next unless File.exists?(filename) and !File.directory?(filename)
        next if @ignored_files.include?(filename)

        @log.info "inotify open event for: #{filename}"
        @ignored_files.add(filename)

        @pool.process {
          obj = @bucket.object(filename[1..-1])
          io = S3reamer::S3WriteStream.new(obj)

          open(filename) do |file|
            stopped = false
            queue = INotify::Notifier.new
            queue.watch(filename, :modify, :close) do |e2|
              b = file.read
              io.write(b)
              @log.debug "Read #{b.length} bytes"

              if e2.flags.include?(:close)
                queue.stop
                stopped = true
              end
            end

            begin
              while !stopped
                Timeout::timeout(1) {
                  @log.debug "Waiting for event..."
                  queue.process
                }
              end
            rescue Timeout::TimeoutError
              @log.warn "Timed out waiting for modify/close on: #{filename}"
              queue.stop
            end

            @log.info "File closed. Completing S3 upload: #{filename}"
          end

          io.close
          @ignored_files.delete(filename)
        }
      end
    end

    def run
      @dir_watch.run
      @pool.shutdown
    end

    def stop
      @dir_watch.stop
    end
  end
end
