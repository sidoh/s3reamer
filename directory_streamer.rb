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
      @log.level = @options[:log_level]
    end

    def stream_directory(directory:, bucket:)
      file_statuses = ThreadSafe::Hash.new
      dir_watch = INotify::Notifier.new
      pool = Thread.pool(options[:pool_size])

      dir_watch.watch(directory, :open, :close, :recursive) do |e|
        filename = e.absolute_name

        log.debug "Events #{e.flags.inspect} received for: #{filename}"

        # Don't process directories
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

          begin
            obj = bucket.object(filename[1..-1])
            io = S3reamer::S3WriteStream.new(obj)
          rescue Error => e
            log.error "Error initializing S3 streamer: #{e}"
            log.error e.backtrace
            raise e
          end

          log.debug "Initialized S3 streamer"

          open(filename) do |file|
            stopped = false
            size = 0
            last_successful_read = Time.now

            # Start with bytes_read != 0 to force at least one read of the file.
            # This addresses the race condition caused by files being opened and
            # closed quickly.
            bytes_read = -1

            # Go until the file has closed, or until we've not seen any new
            # bytes written to the file past some threshold (specified by
            # options[:reader_timeout]).
            while (file_statuses[filename] == :open || bytes_read != 0) &&
              (last_successful_read + options[:reader_timeout]) > Time.now

              b = file.read
              bytes_read = b.length
              io.write(b)

              # If we read any bytes, reset the time at which we last saw new
              # bytes in the file. This prevents the read timeout condition from
              # triggering.
              if bytes_read > 0
                log.debug "Read #{bytes_read} bytes: #{filename}"
                last_successful_read = Time.now
              end

              sleep options[:reader_sleep_interval] unless file_statuses[filename] != :open
            end

            log.info "File closed. Completing S3 upload: #{filename}"
          end

          begin
            io.close
          rescue Error => e
            log.error "Error completing S3 upload: #{e}"
            log.error e.backtrace
          end

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
