module S3reamer
  class S3WriteStream
    DEFAULT_OPTIONS = {
      chunk_size: 5 * 1024 * 1024
    }

    def initialize(object, options = {})
      @buffer = String.new
      @options = DEFAULT_OPTIONS.merge(options)
      @multipart_upload = object.initiate_multipart_upload
      @closed = false
    end

    def write(data)
      raise RuntimeError.new("Illegal state: cannot write after close.") if @closed

      @buffer << data
      flush if @buffer.length >= @options[:chunk_size]
    end

    def flush
      part = @multipart_upload.part(@multipart_upload.parts.count + 1)
      part.upload(body: @buffer)
      @buffer.clear
    end

    def close
      flush unless @buffer.empty?
      @multipart_upload.complete(compute_parts: true)
      @closed = true
    end
  end
end
