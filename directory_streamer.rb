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
      @log.level = Logger::INFO

      @ignored_files = Set.new
      @dir_watch = INotify::Notifier.new
      @pool = Thread.pool(4)

      @dir_watch.watch(@dir, :open, :recursive) do |e|
        filename = e.absolute_name
        next unless File.exists?(filename) and !File.directory?(filename)
        next if @ignored_files.include?(filename)

        log.info "File opened: #{filename}"
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
              log.debug "Read #{b.length} bytes"

              if e2.flags.include?(:close)
                queue.close
                stopped = true
              end
            end

            while !stopped
              if IO.select([queue.to_io], [], [], 30)
                queue.process
              else
                log.warn "Waited for too long for file to be modified/closed."
                stopped = true
              end
            end

            log.info "File closed. Completing S3 upload: #{filename}"
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

    private
      def log
        @log
      end
  end
end
