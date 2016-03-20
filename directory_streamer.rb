require 'set'
require 'logger'
require 'rb-inotify'
require 'aws-sdk'
require 'thread/pool'
require 'timeout'

require_relative 's3_write_stream'

module S3reamer
  class DirectoryStreamer
    DEFAULT_OPTIONS = {
      pool_size: 4,
      close_timeout: 30,
      log_level: Logger::INFO
    }

    attr_reader :options

    def initialize(options = {})
      @options = DEFAULT_OPTIONS.merge(options)
      @log = Logger.new(STDOUT)
      @log.level = @options[:log_level]
    end

    def stream_directory(directory:, bucket:)
      open_files = ThreadSafe::Cache.new
      dir_watch = INotify::Notifier.new
      pool = Thread.pool(options[:pool_size])

      dir_watch.watch(directory, :open, :recursive) do |e|
        filename = e.absolute_name

        next unless File.exists?(filename) and !File.directory?(filename)

        # If this is an "open" event, we should only process it if we haven't
        # already started on this file.
        next if e.flags.include?(:open) and open_files.include?(filename)

        log.info "File opened: #{filename}"
        open_files[filename] = true

        pool.process {
          obj = promise { bucket.object(filename[1..-1]) }
          io = promise { S3reamer::S3WriteStream.new(obj) }

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
              if IO.select([queue.to_io], [], [], options[:close_timeout])
                queue.process
              else
                log.warn "Waited for too long for file to be modified/closed."
                stopped = true
              end
            end

            log.info "File closed. Completing S3 upload: #{filename}"
          end

          io.close
          open_files.delete(filename)
        }
      end

      dir_watch.run
      pool.shutdown
    end

    private
      def log
        @log
      end
  end
end
