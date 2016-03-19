require 'set'
require 'logger'
require 'rb-inotify'
require 'aws-sdk'
require 'thread/pool'

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
            queue = INotify::Notifier.new
            queue.watch(filename, :modify, :close) do |e2|
              b = file.read
              io.write(b)
              @log.debug "Read #{b.length} bytes"

              queue.stop if e2.flags.include?(:close)
            end

            while IO.select([queue.to_io], [], [], [10])
              @log.debug "Got event"
              queue.process
            end

            queue.stop
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
