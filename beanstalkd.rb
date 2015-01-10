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

    # reserves is the number of times this job has been reserved.
    attr_accessor :reserves_count
    # timeouts is the number of times this job has timed out during a reservation.
    attr_accessor :timeouts_count
    # releases is the number of times a client has released this job from a reservation.
    attr_accessor :releases_count
    # buries is the number of times this job has been buried.
    attr_accessor :buries_count
    # kicks is the number of times this job has been kicked.
    attr_accessor :kicks_count

    attr_accessor :delay_timer
    attr_accessor :ttr_timer

    attr_accessor :owner


    TTR = 1
    DELAY = 0

    # Invalid, Delayed, Ready, Reserved, Buried

    def initialize(id: , priority:, delay: DELAY, ttr: TTR, state: :default, value:)
      @id, @priority, @delay, @ttr, @state, @value = id, priority, delay, ttr, state, value
      @created_at, @deadline_at = Time.now, Time.now + delay

      @reserves_count = 0
      @timeouts_count = 0
      @releases_count = 0
      @buries_count = 0
      @kicks_count = 0
    end

    def time_left
      if delayed?
        delay_timer.fire_in.to_i
      elsif reserved?
        ttr_timer.fire_in.to_i
      else
        0
      end
    end

    def delayed?; state == :delayed end
    def reserved?; state == :reserved end
    def ready?; state == :ready end
    def buried?; state == :buried end

    def ready!
      # todo update stats
      # notify tube ?
      case @state
      when :delayed
        # cancel delay timer
      when :ready
        raise 'could not be in ready state already'
      when :released
      when :buried
      when :kicked
        raise NotImplementedError, 'from kicked to ready'
      end
      @state = :ready
    end

    def reserved!
      # set timer
      # change state to reserved
      # todo update stats
      # notify tube ?
      case @state
      when :delayed
        raise 'illegal transition: from delayed to reserved'
      when :ready
        # ok
        # set timers ?
        # update couters ?
      when :released
        # has to be ready
        raise 'illagal state: released to reserved'
      when :buried
        raise 'illagal state: buried to reserved'
      when :kicked
        raise NotImplementedError, 'from kicked to ready'
      end
      @state = :reserved
    end

    def cancel_timers
      self.ttr_timer && self.ttr_timer.cancel
      self.delay_timer && self.delay_timer.cancel
    end
  end

  class Tube
    attr_accessor :name, :jobs

    attr_accessor :urgent_count,
                  :waiting_count,
                  :buried_count,
                  :reserved_count,
                  :pause_count,
                  :total_delete_count,
                  :total_jobs_count

    include Forwardable

    def initialize(name, jobs = [])
      @name = name
      @jobs = jobs
      @watchers = []
      @users = []

      @urgent_count = 0
      @waiting_count = 0
      @buried_count = 0
      @reserved_count = 0
      @pause_count = 0
      @total_delete_count = 0
      @total_jobs_count = 0
    end

    def add_user(client)
      @users << client
    end
    alias_method :add_producer, :add_user

    def add_job(job)
      @jobs << job
    end
    def remove_job(job)
      @jobs.delete job
    end

    def add_watcher(watcher)
      @watchers << watcher
    end
    def remove_watcher(watcher)
      @watchers.delete watcher
    end
    alias_method :add_consumer, :add_watcher
    alias_method :remove_consumer, :remove_watcher
  end

  class Client
    attr_accessor :socket,
                  :current_tube,
                  :watching,
                  :reserve_condition

    def initialize(socket)
      @socket = socket
      @watching = []
      @reserve_condition = Celluloid::Condition.new
      # @current_tube = ?
    end

    def use(tube)
      @current_tube = tube
      tube.add_producer self
    end

    def watch(tube)
      @watching << tube
      tube.add_consumer self
    end

    alias_method :watching_tubes, :watching

    def ignore(tube)
      if @watching.size > 1
        @watching.delete(tube)
        :ignored
      else
        :not_ignored
      end
    end

    module Commands
      class Command; end
      class Put < Command; end
      class Use < Command; end
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
      @ttr_timers = {}
      @tubes = {'default' => @default_tube}
      @clients = []
      @consumers = []

      async.run
    end

    def run
      loop { async.handle_connection @server.accept }
    end
    RN = "\r\n".freeze
    def rn; RN; end

    def add_client(client)
      # todo on disconnect update jobs, stats
      @clients << client
    end
    def handle_connection(socket)
      client = Client.new(socket)
      add_client client

      current_tube = @default_tube
      client.use(current_tube)
      client.watch(current_tube)

      watching_tubes = client.watching_tubes

      _, port, host = socket.peeraddr

      puts "*** Received connection from #{host}:#{port}"
      # todo wrap socket name it client with #read_cmd which returns already build obj
      loop do
        cmd = nil
        # loop {
        #   puts '*'
        #   if socket.eof?
        #     cmd = 'quit'
        #     break
        #   end
        #   unless cmd = socket.gets(/( |\r\n)/)
        #     puts '+'
        #     socket.wait_readable
        #   else
        #     puts '%'
        #     cmd = cmd.chomp(' ').chomp(rn)
        #     break
        #   end
        # }
        # cmd = client.socket.gets(/( |\r\n)/)
        # cmd = 'quit' if cmd.nil? || client.socket.eof?
        # cmd = cmd.chomp(' ').chomp(rn)

        cmd = if client.socket.eof? then 'quit' else client.socket.gets(/( |\r\n)/).chomp(' ').chomp(rn) end
        puts '<< ' + cmd.inspect
        case cmd
        when 'use'
          tube_name = socket.gets(rn).chomp(rn)
          current_tube = find_or_create_tube(tube_name)
          client.use(current_tube)
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
            client.current_tube.add_job job
            job.tube = client.current_tube
            @jobs[jid] = job
            signal_consumer = -> {
              consumer = @consumers.find {|c| c.watching.include?(job.tube)}
              consumer && consumer.reserve_condition.signal
            }

            if delay > 0
              @delay_timers[jid] = job.delay_timer = after(delay) do
                job.ready!
                signal_consumer.call
                # todo signal job ready unless its deleted or so ...
                # todo stats ?
              end
            else
              signal_consumer.call
            end
            socket.write("INSERTED #{jid}" + rn)
          else
            socket.write("EXPECTED_CRLF" + rn)
          end
          # require 'pry'; binding.pry
        when /peek-(ready|delayed|buried)/
          state = cmd[/ready|delayed|buried/].to_sym
          job = client.current_tube.jobs.select {|j| j.state == state}.min_by { |j| [j.priority, j.created_at] }
          if job
            client.socket.write("FOUND #{jid} #{job.value.bytesize}#{rn}#{job.value}#{rn}")
          else
            client.socket.write(NOT_FOUND)
          end
        when 'list-tubes-watched'
          content = client.watching_tubes.map(&:name).to_yaml
          socket.write("OK #{content.bytesize}#{rn}#{content}#{rn}")
        when 'quit', 'q', 'exit'
          raise EOFError
        when 'reload'
          puts 'reloading ...'
          load 'beanstalkd.rb'
        when 'pry'
          binding.pry
        when 'watch'
          tube_name = socket.gets(rn).chomp(rn)
          tube = find_or_create_tube(tube_name)
          # todo update counters
          client.watch(tube)
          socket.write("WATCHING #{client.watching.size}" + rn)
        when 'ignore'
          tube_name = client.socket.gets(rn).chomp(rn)
          tube = find_tube(tube_name)
          # todo update counters
          case client.ignore(tube)
          when :not_ignored
            socket.write('NOT_IGNORED' + rn)
          else
            socket.write("WATCHING #{client.watching.size}" + rn)
          end
        when 'stats-job'
          jid = socket.readline.chomp(rn).to_i

          if job = @jobs[jid]
            now = Time.now

            stats_content = STATS_JOB_FMT % [
              # id is the job id
              job.id,
              # tube is the name of the tube that contains this job
              job.tube.name, # todo check beforehand
              # state is ready or delayed or reserved or buried
              job.state.to_s,
              # pri is the priority value set by the put, release, or bury commands.
              job.priority,
              # age is the time in seconds since the put command that created this job.
              now.to_i - job.created_at.to_i,
              job.delay,
              job.ttr,
              # time-left is the number of seconds left until the server puts this job into the ready queue.
              # This number is only meaningful if the job is reserved or delayed.
              # If the job is reserved and this amount of time elapses before its state changes, it is considered to have timed out.
              0, # todo job.delay_timer.
              # file is the number of the earliest binlog file containing this job. If -b wasn't used, this will be 0.
              0, # todo wal
              # reserves is the number of times this job has been reserved.
              job.reserves_count,
              # timeouts is the number of times this job has timed out during a reservation.
              job.timeouts_count,
              # releases is the number of times a client has released this job from a reservation.
              job.releases_count,
              # buries is the number of times this job has been buried.
              job.buries_count,
              # kicks is the number of times this job has been kicked.
              job.kicks_count
            ]
            client.socket.write "OK #{stats_content.bytesize}" << rn << stats_content << rn
          else
            client.socket.write(NOT_FOUND)
          end
        when 'reserve'
          # block until found
          @consumers << client
          loop {
            job = @jobs.values.select {|j| j.ready? && client.watching.include?(j.tube)}.min_by {|j| [j.priority, j.created_at]}
            if job
              @consumers.delete client
              break
            else
              client.reserve_condition.wait
            end
          }

          # reserve job for client
          job.owner = client
          job.reserved!
          client.socket.write("RESERVED #{job.id} #{job.value.bytesize}#{rn}#{job.value}#{rn}")
        when 'delete'
          id = socket.readline.chomp(rn).to_i
          job = @jobs.delete(id)
          if job && ((job.owner == client && job.reserved?) || job.delayed? || job.buried?)
            job.cancel_timers
            job.owner = nil
            job.tube.remove_job job

            @delay_timers.delete job.id
            @ttr_timers.delete job.id

            client.socket.write('DELETED' + ' ' + job.id.to_s + rn)
          else
            client.socket.write(NOT_FOUND)
          end
        else
          puts "can't handle #{cmd}"
          # client.close
          # abort(cmd + ' ' + socket.readline.inspect)
        end
      end
    rescue EOFError
      puts "*** #{host}:#{port} disconnected"
      socket.close
    end

    def find_or_create_tube(tube_name)
      @tubes[tube_name] || Tube.new(tube_name).tap {|t| @tubes[tube_name] = t}
    end

    def find_tube(tube_name)
      @tubes[tube_name]
    end

    def next_job_id; @jid_seq += 1 end

    STATS_JOB_FMT = "---\n" \
      "id: %d\n" \
      "tube: %s\n" \
      "state: %s\n" \
      "pri: %u\n" \
      "age: %d\n" \
      "delay: %d\n" \
      "ttr: %d\n" \
      "time-left: %d\n" \
      "file: %d\n" \
      "reserves: %u\n" \
      "timeouts: %u\n" \
      "releases: %u\n" \
      "buries: %u\n" \
      "kicks: %u\n"

    def finalize
      @server.close if @server
    end

    NOT_FOUND = "NOT_FOUND\r\n".freeze
  end

  def self.start(argv)
    return if $LOADED
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
    $LOADED = true
    # server.wait nil
    sleep
  end
end

Beanstalkd.start(ARGV) unless $LOADED
# socket.write(['OK ', content.bytesize.to_s, rn, content, rn].join)
# socket.write('OK ', content.bytesize.to_s, rn, content, rn)
# socket.write('OK', ' ', content.bytesize.to_s, rn, content, rn)
# ['OK ', content.bytesize.to_s, rn, content, rn].map(&socket.method(:write))

# client.socket.write("RESERVED", ' ', job.id.to_s, job.value.bitesize, rn, job.value, rn)
#
# client.socket.write("RESERVED %d %d%s%s%s" % [job.id, job.value.bitesize, rn, job.value, rn])
#
# client.socket.write('RESERVED' << ' ' << job.id.to_s << ' ' << job.value.bitesize.to_s << rn << job.value << rn)
#
# client.socket.write(['RESERVED', ' ', job.id.to_s, ' ', job.value.bitesize.to_s, rn, job.value, rn].join)
