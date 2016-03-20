require 'set'
require 'logger'
require 'rb-inotify'
require 'aws-sdk'
require 'thread/pool'
require 'concurrent'
require 'thread_safe'

require_relative 's3_write_stream'

module S3reamer
  class DirectoryStreamer
    DEFAULT_OPTIONS = {
      pool_size: 4,
      log_level: Logger::INFO,
      reader_sleep_interval: 1,
      reader_timeout: 10
    }

    attr_reader :options

    def initialize(options = {})
      @options = DEFAULT_OPTIONS.merge(options)
      @log = Logger.new(STDOUT)
      # @log.level = Logger::DEB@options[:log_level]
    end

    def stream_directory(directory:, bucket:)
      file_statuses = ThreadSafe::Hash.new
      dir_watch = INotify::Notifier.new
      pool = Thread.pool(options[:pool_size])

      dir_watch.watch(directory, :open, :close, :recursive) do |e|
        filename = e.absolute_name

        next unless File.exists?(filename) and !File.directory?(filename)

        # If this is an "open" event, we should only process it if we haven't
        # already started on this file.
        next if e.flags.include?(:open) and file_statuses.include?(filename)

        # If this is a "close" event, we should update the status to inform the
        # worker thread
        if e.flags.include?(:close) and file_statuses.include?(filename)
          file_statuses[filename] = :close
          next
        end

        log.info "File opened: #{filename}"
        file_statuses[filename] = :open

        pool.process {
          log.debug "Starting process for: #{filename}"

          obj = bucket.object(filename[1..-1])
          io = S3reamer::S3WriteStream.new(obj)

          log.debug "Initialized S3 streamer"

          open(filename) do |file|
            stopped = false
            size = 0
            start_time = Time.now

            while file_statuses[filename] == :open &&
              (start_time + options[:reader_timeout]) > Time.now

              b = file.read
              io.write(b)
              log.debug "Read #{b.length} bytes"

              sleep options[:reader_sleep_interval]
            end

            log.info "File closed. Completing S3 upload: #{filename}"
          end

          io.close
          file_statuses.delete(filename)
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
