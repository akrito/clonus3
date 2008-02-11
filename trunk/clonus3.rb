#!/usr/bin/env ruby

# clonus3: a simple backup tool for Amazon's S3 service
# usage: clonus3.rb --help

# TODO
# - funky characters in names aren't supported very well
# - optionally gzip & set content-encoding
# - optionally set content-type

# For S3 access in irb:
# require 'rubygems'
# require 'right_aws'
# y = YAML.load_file FILE.yml
# client = RightAws::S3Interface.new(y['access_key_id'], y['secret_access_key'], :port => 80, :protocol => 'http')

require 'rubygems'
require 'yaml'
require 'right_aws'
require 'optparse'

class BackupActor
    
    def say(str)
        if @options[:verbosity] >= 1
            print str
            STDOUT.flush
        end
    end

    # Connect to S3.
    def initialize(options)
      
        @options = options
        @settings = YAML.load_file(@options[:settings_file])
        @bucket_name = @settings['bucket']
        
        @log = Logger.new(STDERR)
        
        case @options[:verbosity]
        when 0
            @log.level = Logger::ERROR
        when 1
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
    end

    def backup
    
        # Create and/or update the bucket's acl
        if @settings['bucket_acl']
            @client.create_bucket(@bucket_name, 'x-amz-acl' => @settings['bucket_acl'])
        else
            @client.create_bucket(@bucket_name)
        end
        
        if @options[:delete]
            delete
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

                    begin
                        if @settings['relative_paths']
                            headers = @client.head(@bucket_name, rel_path)
                        else
                            # strip the leading "/" from path
                            headers = @client.head(@bucket_name, path[1..-1])
                        end
                        
                        if (mtime != headers['x-amz-meta-mtime']) or (size != headers['content-length'])
                            timed_store(root, rel_path, "+ Updating #{path} - #{size}")
                        else
                            say '.'
                        end

                    rescue RightAws::AwsError # path doesn't exist on S3
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

        if @settings['relative_paths']
            aws_path = rel_path
        else
            aws_path = abs_path[1..-1]
        end
            
        request_headers = { 'x-amz-meta-mtime' => mtime }
        if @settings['object_acl']
            request_headers['x-amz-acl'] = @settings['object_acl']
        end
        
        say "\n" + text

        if not @options[:dryrun]
            t1 = Time.now
            @client.put(@bucket_name, aws_path, File.open(abs_path), request_headers)
            t = Time.now - t1
    
            say " in %.2fs [%.2fKB/s]" % [ t, (size.to_i / 1000.0) / t ]
        else
            say " (dry run)"
        end
    end

    def delete
        say "  Cleaning S3: "
        marker = ''
        loop do
            resp = @client.list_bucket(@bucket_name, { 'marker' => marker })
            break if resp == []
            say '.'
            marker = resp.last[:key]

            resp.each do |obj|
                if not File.exist?(abs_path_from_key(obj[:key]))
                    say "\n- Removing %s" % obj[:key]
                    if @options[:dryrun]
                        say " (dry run)"
                    else
                        @client.delete(@bucket_name, obj[:key])
                    end
                end
            end
        end
    end

    def abs_path_from_key(key)
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

# Parse command-line options
OptionParser.new do |opts|
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
    
    opts.on("-h", "--help", "Show this message") do
        puts opts
        exit 1
    end
    
end.parse!

# If it's not an option, it's the settings file
options[:settings_file] = ARGV.first

b = BackupActor.new(options)
b.backup
