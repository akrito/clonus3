#!/usr/bin/env ruby

# clonus3: a simple backup tool for Amazon's S3 service
# usage: clonus3.rb --help

# Requires: right_aws -- http://rightaws.rubyforge.org/
#           bdb bindings for caching -- http://moulon.inra.fr/ruby/bdb.html

# TODO
# - funky characters in names don't work: "?", "+".  Should we URI-encode them?
# - optionally gzip & set content-encoding
# - optionally set content-type
# - Should --delete verify the object does not match an "ignore"
#   pattern?  Should it verify the file path for the object is a
#   subdirectory of a root?
# - Make --delete work with relative paths AND multiple roots
# - md5 support
# - Allow single files to be specified in roots.

# For S3 access in irb:
# require 'rubygems'
# require 'right_aws'
# y = YAML.load_file FILE.yml
# client = RightAws::S3Interface.new(y['access_key_id'], y['secret_access_key'], :port => 80, :protocol => 'http')

require 'rubygems'
require 'yaml'
require 'right_aws'
require 'optparse'
require 'fileutils'

class BackupActor

    def initialize(options)
      
        # command-line options
        @options = options

        # settings from the config file
        @settings = YAML.load_file(@options[:settings_file])

        @bucket_name = @settings['bucket']
        
        @log = Logger.new(STDERR)
        case @options[:verbosity]
        when 0,1
            @log.level = Logger::ERROR
        when 2
            @log.level = Logger::DEBUG
        end

        @client = RightAws::S3Interface.new(
            @settings['access_key_id'],
            @settings['secret_access_key'],
            :port => 80,
            :protocol => 'http',
            :logger => @log
        )

        db_location = @settings['cache']
        if db_location
            require 'dbm'
            @db = DBM.new(@settings['cache'])
        end
    end

    def say(str)
        if @options[:verbosity] >= 1
            print str
            STDOUT.flush
        end
    end

    def s3path(root, rel_path)
        if @settings['relative_paths']
            return rel_path
        else
            abs_path = root + '/' + rel_path
            return abs_path[1..-1]
        end
    end

    # Perform a head request, optionally caching
    def head(root, rel_path)
        
        # First, check the cache
        if @settings['cache']
            headers_yaml = @db[s3path(root, rel_path)]
            if headers_yaml
                return YAML.load(headers_yaml)
            end
        end

        # If no cache hit, HEAD S3
        begin
            headers = @client.head(@bucket_name, s3path(root, rel_path))
        rescue RightAws::AwsError # path doesn't exist on S3
            headers = nil
        end

        # Save headers to the cache
        if @settings['cache'] and headers
            @db[s3path(root, rel_path)] = YAML.dump(headers)
        end

        return headers
    end

    def backup
    
        # Create and/or update the bucket and its acl
        if @settings['bucket_acl']
            @client.create_bucket(@bucket_name, 'x-amz-acl' => @settings['bucket_acl'])
        else
            @client.create_bucket(@bucket_name)
        end
        
        # Only list the contents of the bucket if we have to
        if @options[:delete] or (@settings['cache'] and @options[:rebuildcache])
            scan_s3
        end

        @settings['roots'].sort.each do |root|
            walk(root, '')
        end
        
        say "\n"
    end

    def walk(root, rel_dir)
        if rel_dir != ''
            dir = root + '/' + rel_dir
        else
            dir = root
        end

        say "\n  Scanning %s" % dir

        begin
            entries = Dir.entries(dir).sort
        rescue
            @log.error("error reading directory #{dir}: " + $!)
            return
        end

        entries.sort.each do |entry|
            if rel_dir != ''
                rel_path = rel_dir + '/' + entry
            else
                rel_path = entry
            end
            path = root + '/' + rel_path

            if entry == '.' or entry == '..' # bogus entries
                next
            elsif File.symlink?(path) # not a real file - exists elsewhere
                next
            elsif @settings['ignore'].find{|pat|path.match(pat)} # ignored files
                next
            elsif File.directory?(path)
                walk(root, rel_path)
            elsif File.file?(path)
                begin # to catch permission errors

                    mtime = File.mtime(path).to_i.to_s
                    size = File.size(path).to_s

                    headers = head(root, rel_path)
                    if headers
                        if (mtime != headers['x-amz-meta-mtime']) or (size != headers['content-length'])
                            timed_store(root, rel_path, "+ Updating #{path} - #{size}")
                        else
                            say '.'
                        end
                    else
                        timed_store(root, rel_path, "! Uploading #{path} - #{size}")
                    end
                    
                rescue
                    @log.error("Could not back up #{path}: " + $!)
                end
            end
        end
    end

    def timed_store(root, rel_path, text)
        abs_path = root + '/' + rel_path
        size = File.size(abs_path)
        mtime = File.mtime(abs_path).to_i

        request_headers = { 'x-amz-meta-mtime' => mtime }
        if @settings['object_acl']
            request_headers['x-amz-acl'] = @settings['object_acl']
        end
        
        say "\n" + text

        if not @options[:dryrun]
            
            # Clear the cache
            if @settings['cache']
                @db.delete(s3path(root, rel_path))
            end

            t1 = Time.now
            @client.put(@bucket_name, s3path(root, rel_path), File.open(abs_path), request_headers)
            t = Time.now - t1
    
            say " in %.2fs [%.2fKB/s]" % [ t, (size.to_i / 1000.0) / t ]
        else
            say " (dry run)"
        end
    end

    def scan_s3
        say "  Listing bucket: "
        
        if @settings['cache'] and @options[:rebuildcache]
            newdb_name = @settings['cache'] + '.tmp'
            newdb = DBM.new(newdb_name)
        end
        
        marker = ''
        loop do
            resp = @client.list_bucket(@bucket_name, { 'marker' => marker })
            break if resp == []
            say '.'
            marker = resp.last[:key]

            resp.each do |obj|

                key = obj[:key]

                # Does the S3 object still exist on the filesystem?

                if @options[:delete]

                    # TODO: make sure the file is in a valid root and
                    # doesn't match an ignored regex

                    if not File.exist?(file_path_from_key(key))
                        say "\n- Unstoring %s" % key
                        if @options[:dryrun]
                            say " (dry run)"
                        else
                            # Clear the cache
                            if @settings['cache']
                                @db.delete key
                            end
                            
                            @client.delete(@bucket_name, key)
                        end
                    end
                end

                # Remove out-of-date cached HEAD responses

                if @settings['cache'] and @options[:rebuildcache]
                    cached_obj_yaml = @db[key]
                    if cached_obj_yaml
                        cached_obj = YAML.load(cached_obj_yaml)

                        if cached_obj and cached_obj['etag'] == obj[:e_tag]
                            newdb[key] = cached_obj_yaml
                        else
                            @log.error "S3 does not match cache.  Who messed with my bucket?"
                        end
                    end
                end
            end
        end

        # set @db to the new cache

        if @settings['cache'] and @options[:rebuildcache]
            newdb.close
            @db.close
            FileUtils.mv(newdb_name + '.db', @settings['cache'] + '.db')
            @db = DBM.new(@settings['cache'])
        end
    end

    def file_path_from_key(key)
        if @settings['relative_paths'] # only works if there's a single root
            abs_path = @settings['roots'].first + '/' + key
        else
            abs_path = '/' + key
        end

        return abs_path
    end
end

# Set up default options
options = {}
options[:verbosity] = 1
options[:rebuildcache] = true

# Parse command-line options
opts = OptionParser.new do |opts|
    opts.banner = "Usage: clonus3.rb [options] YAML_FILE"

    opts.on("-q", "--quiet", "Only show errors") do |q|
        options[:verbosity] = 0
    end

    opts.on("-v", "--verbose", "Run verbosely") do |v|
        options[:verbosity] = 2
    end
  
    opts.on("-d", "--delete", "First, remove files which won't be backed up from S3") do |d|
        options[:delete] = d
    end
    
    opts.on("-n", "--dry-run", "Connect to S3 and create the bucket, but don't upload or remove files") do |n|
        options[:dryrun] = n
    end
    
    opts.on("-r", "--[no-]rebuild-cache", "Rebuild the cache, tossing out stale HEAD responses.  --rebuild-cache is the default") do |r|
        options[:rebuildcache] = r
    end

    opts.on("-h", "--help", "Show this message") do
        puts opts
        exit 1
    end
    
end

opts.parse!

# If it's not an option, it's the settings file.
# If no settings file is given, error.
if ARGV.first
    options[:settings_file] = ARGV.first
else
    puts opts
    exit
end

b = BackupActor.new(options)
b.backup
