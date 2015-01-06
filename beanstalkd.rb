require 'rubygems'
require 'optparse'
require 'celluloid/io'
require 'yaml'

require 'pry'

module Beanstalkd
  # VERSION = 'unknown'

  class Job
    attr_accessor :id, :priority, :delay, :ttr, :state, :value
    attr_accessor :created_at, :deadline_at

    attr_accessor :tube

    TTR = 1
    DELAY = 0

    def initialize(id: , priority:, delay: DELAY, ttr: TTR, state: :default, value:)
      self.id, self.priority, self.delay, self.ttr, self.state, self.value = id, priority, delay, ttr, state, value
      self.created_at, self.deadline_at = Time.now, Time.now + delay
    end
  end

  class Tube
    attr_accessor :name, :jobs

    include Forwardable

    def initialize(name, jobs = [])
      self.name = name
      self.jobs = jobs
    end

    def <<(job)
      @jobs << job
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

      @default_tube = Tube.new('default')
      @jid_seq = 0
      @jobs = {}
      @delay_timers = {}
      @tubes = {}

      async.run
    end

    def run
      loop { async.handle_connection @server.accept }
    end
    RN = "\r\n".freeze
    def rn; RN; end

    def handle_connection(socket)
      # require 'pry'; binding.pry
      current_tube = @default_tube
      watching_tubes = [@default_tube]
      _, port, host = socket.peeraddr
      puts "*** Received connection from #{host}:#{port}"
      # todo wrap socket name it client with #read_cmd which returns already build obj
      loop do
        cmd = socket.gets(/( |\r\n)/, 50).chomp(' ').chomp(rn)
        puts cmd.inspect
        case cmd
        when 'use'
          tube_name = socket.gets(rn).chomp(rn)
          current_tube = @tubes[tube_name] || Tube.new(tube_name).tap {|t| @tubes[tube_name] = t}
          socket.write("USING #{tube_name}" + rn)
        when 'put'
          priority, delay, ttr, bytes = socket.readline.chomp(rn).split(' ').map(&:to_i)
          body = socket.read(bytes)
          if bytes > 65535
            while bytes != 0
              size = 1024
              if bytes > 1024 then bytes =- 1024 else size = bytes end
              socket.read(size)
            end
            socket.read(2) # rn
            socket.write("JOB_TOO_BIG" + rn)
            # todo throw away bytes from socket + nr
          elsif socket.read(2) == rn
            jid = next_job_id
            job = Job.new(id: jid, priority: priority, delay: delay, ttr: ttr, state: (if delay > 0 then :delayed else :ready end), value: body)
            current_tube << job
            @jobs[jid] = job
            if delay > 0
              @delay_timers[jid] = after(delay) do
                job.state = :ready
                # todo signal job ready unless its deleted or so ...
                # todo stats ?
              end
            end
            socket.write("INSERTED #{jid}" + rn)
          else
            socket.write("EXPECTED_CRLF" + rn)
          end
          # require 'pry'; binding.pry
        when 'peek-ready'
          job = current_tube.jobs.select {|j| j.state == :ready}.sort_by { |j| j.priority }.first
          job.state = :reserved
          # todo remember by whom on distonnect change state to ready
          @ttr_timers[jid] = after(job.ttr) do
            if @jobs[:jid]
              job.state = :ready
            end
          end
          # FOUND <id> <bytes>\r\n <data>\r\n
          socket.write("FOUND #{jid} #{job.value.bytesize}#{rn}#{job.value}#{rn}")
        when 'list-tubes-watched'
          content = watching_tubes.map(&:name).to_yaml
          socket.write("OK #{content.bytesize}#{rn}#{content}#{rn}")
        when 'quit', 'q', 'exit'
          raise EOFError
        else
          puts cmd + ' ' + socket.readline.inspect
        end
      end
    rescue EOFError
      puts "*** #{host}:#{port} disconnected"
      socket.close
    end

    def next_job_id; @jid_seq += 1 end


    def finalize
      @server.close if @server
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
# socket.write(['OK ', content.bytesize.to_s, rn, content, rn].join)
# socket.write('OK ', content.bytesize.to_s, rn, content, rn)
# socket.write('OK', ' ', content.bytesize.to_s, rn, content, rn)
# ['OK ', content.bytesize.to_s, rn, content, rn].map(&socket.method(:write))
