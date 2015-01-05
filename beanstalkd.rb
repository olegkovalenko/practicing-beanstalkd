require 'rubygems'
require 'optparse'
require 'celluloid/io'

module Beanstalkd
  # VERSION = 'unknown'

	class Job
		attr_accessor :id, :priority, :delay, :ttr, :state, :value
		attr_accessor :created_at, :deadline_at

		TTR = 1
		DELAY = 0

		def initialize(id: , priority:, delay: DELAY, ttr: TTR, state: :default, value:)
			self.id, self.priority, self.delay, self.ttr, self.state, self.value = id, priority, delay, ttr, state, value
			self.created_at, self.deadline_at = Time.now, Time.now + delay
		end
	end

	class Tube
		attr_accessor :name
		attr_accessor :jobs
		def initialize(name:, generator:)
			self.name = name
		end

		def put(cmd)
			job = Job.new(id: generator.uniq_id, priority: cmd.priority, delay: cmd.delay, ttr: cmd.ttr, state: cmd.delayed? ? :delayed : :ready)
			# TODO set timer delay seconds then change to ready state
			jobs << job
		end
	end

	module Commands
		Put = Struct.new(:priority, :delay, :ttr, :value, :tube_name) do
			Inserted = Struct.new(:id)
			Buried = Struct.new(:id)
			Draining = Class.new

			def delayed?; delay > 0 end
		end
		Reserve = Struct.new(:tube_names, :timeout) do
			def with_timeout?; timeout > 0 end
		end
	end

	class Server
		include Celluloid::IO

		def initialize(options)
			host = options.fetch :host
			port = options.fetch :port
			puts "*** Starting echo server on #{host}:#{port}"

			@config = options
			@server = TCPServer.new(options[:host], options[:port])

			async.run
		end

		def run
			loop { async.handle_connection @server.accept }
		end

		def handle_connection(socket)
			_, port, host = socket.peeraddr
			puts "*** Received connection from #{host}:#{port}"
			loop { puts socket.readpartial(4096) }
		rescue EOFError
			puts "*** #{host}:#{port} disconnected"
			socket.close
		end
	end

  def self.start(argv)
    # parse command line options
    options = {host: '0.0.0.0', port: 11300, job_max_data_size: 65535, wal_max_file_size: 10485760, wal_compact: true}

    OptionParser.new(nil, 8, '  ') do |parser|
      parser.separator ''
      parser.separator 'Options:'

      parser.on('-b DIR', 'wal directory') { |dir| options[:wal_dir] = dir }
      parser.on('-f MS', 'fsync at most once every MS milliseconds (use -f0 for "always fsync")') { |fsync_rate|
        options[:fsync_rate] = fsync_rate
        options[:want_sync] = true
      }
      parser.on('-F', 'never fsync (default)') { |fsync_rate| options[:fsync_rate] = fsync_rate }
      parser.on('-l ADDR', "listen on address (default is #{options[:host]})") { |host| options[:host] = host }
      parser.on('-p PORT', "listen on port (default is #{options[:port]})") { |port| options[:port] = port.to_i }
#  -u USER  become user and group TODO
      parser.on('-z BYTES', "set the maximum job size in bytes (default is #{options[:job_max_data_size]})") { |max|
        options[:job_max_data_size] = max.to_i
      }
      wal_size_desc = "set the size of each wal file (default is #{options[:wal_max_file_size]})\n" \
           "             (will be rounded up to a multiple of 512 bytes)"
      parser.on('-s BYTES', wal_size_desc) { |max| options[:wal_max_file_size] = max.to_i }
      parser.on('-c', 'compact the binlog (default)') { options[:wal_compact] = true }
      parser.on('-n', 'do not compact the binlog') { options[:wal_compact] = false }
      parser.on('-v', 'show version information') { puts "beanstalkd #{VERSION}"; exit(0) }
      parser.on('-V', 'increase verbosity') { options[:verbose] = true }
      parser.on('-h', 'show this help') { puts parser; exit(0) }
    end.parse(argv)

    puts options.inspect

		server = Server.new(options)
		sleep
  end
end

Beanstalkd.start(ARGV)
