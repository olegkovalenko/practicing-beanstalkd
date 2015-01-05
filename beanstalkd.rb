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
		def ready_after(job)
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
		attr_accessor :tubes
		attr_accessor :jobs
		DEFAULT_TUBE = Tube.new(name: 'default')
		def initialize
			@tubes = [DEFAULT_TUBE]
			@job_id_seq = 0
		end
		# put <pri> <delay> <ttr> <data>
		def put(cmd)
			tube = find_or_create_tube(cmd.tube_name)
			tube.put(cmd)
		end
		def reserve(tube_names)
			tubes.find {|t| t.reserve if tube_names.include?(t.name) && !t.empty?}
		end
		def find_or_create_tube(name)
			tubes.find {|t| t.name == name} || Tube.new(name).tap {|t| tubes << t}
		end

		def uniq_id; self.uniq_id_seq += 1 end
	end

  def self.beanstalkd(argv)
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

		market = Market.new

		p1 = market.put(Commands::Put.new(5, 0, 30, 'p1v', 'default'))
		p2 = market.put(Commands::Put.new(5, 0, 30, 'p2v', 'default'))
		p3 = market.put(Commands::Put.new(5, 0, 30, 'p3v', 'default'))


#
# setup logging
# set owner setgid/setuid
#
# tcp server family PF_UNSPEC, type SOCK_STREAM, flags AI_PASSIVE
# socket +| O_NONBLOCK
# r = setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &flags, sizeof flags);
# r = setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &flags, sizeof flags);
# r = setsockopt(fd, SOL_SOCKET, SO_LINGER, &linger, sizeof linger);
# r = setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flags, sizeof flags);
# bind fd, addr
# listen fd, 1024
#
# set signal handlers sigpipe, usr1
#
# wal, lock dir
  end
end

Beanstalkd.beanstalkd(ARGV)
