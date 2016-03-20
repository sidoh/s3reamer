$:.push File.expand_path('../lib', __FILE__)

require "s3reamer/version"

Gem::Specification.new do |gem|
  gem.name    = 's3reamer'
  gem.version = S3reamer::VERSION

  gem.summary = "Automatically upload files to S3 as they're created"

  gem.authors  = ['Christopher Mullins']
  gem.email    = 'chris@sidoh.org'
  gem.homepage = 'http://github.com/sidoh/s3reamer'

  gem.add_dependency 'rake'
  gem.add_dependency 'aws-sdk', '~> 2'
  gem.add_dependency 'rb-inotify', '~> 0.9'
  gem.add_dependency 'thread', '~> 0.2'
  gem.add_dependency 'thread_safe', '~> 0.3'
  gem.add_dependency 'concurrent-ruby', '~> 1'

  ignores  = File.readlines(".gitignore").grep(/\S+/).map(&:chomp)
  dotfiles = %w[.gitignore]

  all_files_without_ignores = Dir["**/*"].reject { |f|
    File.directory?(f) || ignores.any? { |i| File.fnmatch(i, f) }
  }

  gem.files = (all_files_without_ignores + dotfiles).sort
  gem.executables = ["s3reamer"]

  gem.require_path = "lib"
end
