require 'set'
require 'logger'
require 'rb-inotify'
require 'aws-sdk'

require_relative 's3_write_stream'

module S3reamer
  class DirectoryStreamer
    def initialize(dir, bucket)
      @dir = dir
      @bucket = bucket
      @log = Logger.new(STDOUT)

      @ignored_files = Set.new
      @dir_watch = INotify::Notifier.new
      @dir_watch.watch(@dir, :open, :recursive) do |e|
        filename = e.absolute_name
        if @ignored_files.include?(filename)
          log.debug "ignoring event for : #{filename}"
          @ignored_files.delete(filename)
          next
        end

        @log.info "inotify open event for: #{filename}"
        obj = @bucket.object(filename)
        io = S3reamer::S3WriteStream(obj)

        open(filename) do |file|
          queue = INotify::Notifier.new
          queue.watch(filename, :modify, :close) do |e2|
            b = file.read
            io.write(b)
            log.debug "Read #{b.length} bytes"

            queue.stop if e2.flags.include?(:close)
          end

          queue.run
        end
      end
    end

    def run
      @dir_watch.run
    end

    def stop
      @dir_watch.stop
    end
  end
end

ignored = Set.new
log = Logger.new(STDOUT)
dir = ARGV[0]
bucket = ARGV[1]

s3 = Aws::S3::Resource.new(region: 'us-west-2')

dir_watch = INotify::Notifier.new
dir_watch.watch(dir, :open, :recursive) do |e|
  filename = e.absolute_name
  if ignored.include?(filename)
    log.debug "ignoring event  for : #{filename}"
    ignored.delete(filename)
    next
  end

  log.info "inotify open event for: #{filename}"

  io = StringIO.new
  obj = s3.bucket(bucket).object(e.absolute_name)
  obj.upload_file(io)

  open(filename) do |file|
    file.seek(0, IO::SEEK_END)

    queue = INotify::Notifier.new
    queue.watch(filename, :modify, :close) do |e2|
      b = file.read
      io.write(b)
      log.debug "Read #{b.length} bytes"

      queue.stop if e2.flags.include?(:close)
    end

    queue.run
  end

  io.close
  log.info "Done with #{filename}!"
  ignored << filename
end
dir_watch.run
