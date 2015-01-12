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

    attr_reader :actor

    # include Celluloid::FSM

    # https://github.com/celluloid/timers/blob/master/lib/timers/timer.rb
    # http://ruby-doc.org/stdlib-1.9.3/libdoc/observer/rdoc/Observable.html#method-i-changed
    # states: default, delayed, ready, reserved, buried, invalid
    # invalid aka deleted
    #
    # state :default, to: :ready do
    #   # tube.job_state_change(:default, :ready, self)
    #   actor.job_state_change(:default, :ready, self)
    # end

    # state :default, to: :delayed do
    #   @delay_timer = actor.after(delay) do
    #     transition :ready
    #   end
    # end

    def illegal_transition(from, to)
      raise 'Illegal transition from %s to %s' % [from, to]
    end

    def delayed!
      case @state
      when :default
        @delay_timer = actor.after(delay) do
          ready!
        end
      when :delayed
        illegal_transition :delayed, :delayed
      when :ready
        illegal_transition :ready, :delayed
      when :reserved
        illegal_transition :reserved, :delayed
      when :buried
        illegal_transition :buried, :delayed
      when :invalid then
        illegal_transition :invalid, :delayed
      end

      # much easier to read
      # case [@state   , :delayed]
      # when [:default , :delayed]
      # when [:delayed , :delayed]
      # when [:ready   , :delayed]
      # when [:reserved, :delayed]
      # when [:buried  , :delayed]
      # when [:invalid , :delayed]
      # end

      @state = :delayed
    end

    def ready!
      case state
      when :default
        actor.job_state_change(:default, :ready, self)

      when :delayed
        @delay_timer.cancel
        actor.job_state_change(:delayed, :ready, self)

      when :ready
        illegal_transition :ready, :ready

      when :reserved
        @ttr_timer.cancel
        @deadline_at = nil

        @timeouts_count += 1

        # TODO client disconnect or ttr timeout
        actor.job_state_change(:reserved, :ready, self)

      when :buried
        illegal_transition :buried, :ready
      when :invalid then
        illegal_transition :invalid, :ready
      end

      @state = :ready
    end

    def reserved!
      case state
      when :default
        illegal_transition :delayed, :reserved
      when :delayed
        illegal_transition :delayed, :reserved
      when :ready
        @ttr_timer = actor.after(ttr) do
          ready!
        end
        @deadline_at = Time.now + ttr
        @reserves_count += 1

        # TODO
        # job.tube.waiting_count
        # job.tube.reserved_count

        actor.job_state_change(:ready, :reserved, self)
        # actor.job_state_change(READY, RESERVED, self)
      when :reserved
        illegal_transition :reserved, :reserved
      when :buried
        illegal_transition :buried, :reserved
      when :invalid then
        illegal_transition :invalid, :reserved
      end

      @state = :reserved
    end

    # state :delayed, to: :ready do
    #   @delay_timer.cancel
    #   actor.job_state_change(:delayed, :ready, self)
    # end

    # state Ready, to: Reserved do
    # state Job.ready, to: Job.reserved do
    # state READY, to: RESERVED do
    # state :ready, to: :reserved do
    #   @ttr_timer = actor.after(ttr) do
    #     transition :ready
    #   end
    #   @deadline_at = Time.now + ttr
    #   @reserves_count += 1

    #   # TODO
    #   # job.tube.waiting_count
    #   # job.tube.reserved_count

    #   actor.job_state_change(:ready, :reserved, self)
    #   # actor.job_state_change(READY, RESERVED, self)
    # end
    # state :ready, to: :delayed do end
    # state :ready, to: :buried do end

    # state :reserved, to: :ready do
    #   @ttr_timer.cancel
    #   @deadline_at = nil

    #   @timeouts_count += 1

    #   # TODO client disconnect or ttr timeout
    #   actor.job_state_change(:reserved, :ready, self)
    # end
    # state :reserved, to: :delayed do raise 'Not implemented' end
    # state :reserved, to: :buried do raise 'Not implemented' end

    # state :buried, to: :delayed do raise 'Not implemented' end
    # state :buried, to: :ready do raise 'Not implemented' end
    # state :buried, to: :reserved do raise 'Not implemented' end


    def ttr_timeout
      ready!
    end

    def start
      if delay > 0
        delayed!
      else
        ready!
      end
    end


    TTR = 1
    DELAY = 0

    # Invalid, Delayed, Ready, Reserved, Buried

    def initialize(id: , priority:, delay: DELAY, ttr: TTR, state: :default, value:)
      @id, @priority, @delay, @ttr, @state, @value = id, priority, delay, ttr, state, value

      @created_at  = Time.now
      # @deadline_at = @created_at + delay

      @reserves_count = 0
      @timeouts_count = 0
      @releases_count = 0
      @buries_count = 0
      @kicks_count = 0

      @actor = Celluloid::Actor.current
    end

    def time_left
      if delayed?
        delay_timer.fires_in.to_i
      elsif reserved?
        ttr_timer.fires_in.to_i
      else
        0
      end
    end

    def urgent?; priority < 1024 end

    def delayed?; state == :delayed end
    def reserved?; state == :reserved end
    def ready?; state == :ready end
    def buried?; state == :buried end

    # def ready!
    #   # TODO update stats
    #   # notify tube ?
    #   case @state
    #   when :delayed
    #     # cancel delay timer
    #   when :ready
    #     raise 'could not be in ready state already'
    #   when :released
    #   when :buried
    #   when :kicked
    #     raise NotImplementedError, 'from kicked to ready'
    #   end
    #   @state = :ready
    # end

    # def reserved!
    #   # set timer
    #   # change state to reserved
    #   # TODO update stats
    #   # notify tube ?
    #   case @state
    #   when :delayed
    #     raise 'illegal transition: from delayed to reserved'
    #   when :ready
    #     # ok
    #     # set timers ?
    #     # update couters ?
    #   when :released
    #     # has to be ready
    #     raise 'illagal state: released to reserved'
    #   when :buried
    #     raise 'illagal state: buried to reserved'
    #   when :kicked
    #     raise NotImplementedError, 'from kicked to ready'
    #   end
    #   @state = :reserved
    # end

    def cancel_timers
      self.ttr_timer && self.ttr_timer.cancel
      self.delay_timer && self.delay_timer.cancel
    end

    def distance
      [priority, created_at]
    end


    # TODO consider using FSM
    def delete
      case state
      when :default
      when :delayed
        delay_timer.cancel
      when :ready
      when :reserved
        ttr_timer.cancel
        owner.remove_job self
      when :buried
      end
      @deadline_at = nil

      tube.remove_job self

      @state = :invalid

      # TODO job.tube.total_jobs_count
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

    def add_user(user)
      @users << user
    end
    def remove_user(user)
      @users.delete user
    end
    alias_method :add_producer, :add_user
    alias_method :remove_producer, :remove_user

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
                  :reserve_condition,
                  :jobs

    include Celluloid::FSM

    default_state :connected

    state :connected, to: :disconnected do
      @current_tube.remove_producer(self) if @current_tube
      @current_tube = nil

      @watching.each {|t| t.remove_watcher(self)}
      @watching.clear
      @watching = nil

      @jobs.each {|j| j.ttr_timeout}
      @jobs.clear
      @jobs = nil

      @socket.close

      # TODO # @reserve_condition.broadcast # exception = ConditionError.new("timeout after #{timeout.inspect} seconds")
    end

    state :disconnected

    def initialize(socket)
      @socket = socket
      @watching = []
      @reserve_condition = Celluloid::Condition.new
      @jobs = []
      # @current_tube = ?
    end

    def use(tube)
      @current_tube.remove_producer(self) if @current_tube
      @current_tube = tube
      @current_tube.add_producer self
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

    def add_job(job)
      @jobs << job
    end

    def remove_job(job)
      @jobs.delete(job)
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
      # TODO update jobs, stats
      @clients << client
    end
    def remove_client(client)
      # TODO update jobs, stats
      @clients.delete client
    end
    def handle_connection(socket)
      client = Client.new(socket)
      add_client client

      current_tube = @default_tube
      client.use(current_tube)
      client.watch(current_tube)

      _, port, host = socket.peeraddr

      puts "*** Received connection from #{host}:#{port}"
      # TODO wrap socket name it client with #read_cmd which returns already build obj
      loop do
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
            socket.write('JOB_TOO_BIG' + rn)
          elsif socket.read(2) == rn
            jid = next_job_id
            job = Job.new(id: jid, priority: priority, delay: delay, ttr: ttr, value: body)
            job.tube = client.current_tube
            job.tube.add_job job
            @jobs[jid] = job
            job.start
            # signal_consumer = -> {
            #   consumer = @consumers.find {|c| c.watching.include?(job.tube)}
            #   consumer && consumer.reserve_condition.signal
            # }

            # if delay > 0
            #   @delay_timers[jid] = job.delay_timer = after(delay) do
            #     job.ready!
            #     signal_consumer.call
            #     # TODO signal job ready unless its deleted or so ...
            #     # TODO stats ?
            #   end
            # else
            #   signal_consumer.call
            # end
            socket.write("INSERTED #{jid}" + rn)
          else
            socket.write("EXPECTED_CRLF" + rn)
          end
          # require 'pry'; binding.pry
        when /peek-(ready|delayed|buried)/
          state = cmd[/ready|delayed|buried/].to_sym
          job = client.current_tube.jobs.select {|j| j.state == state}.min_by { |j| j.distance }
          if job
            client.socket.write("FOUND #{jid} #{job.value.bytesize}#{rn}#{job.value}#{rn}")
          else
            client.socket.write(NOT_FOUND)
          end
        when 'list-tubes-watched'
          content = client.watching.map(&:name).to_yaml
          socket.write("OK #{content.bytesize}#{rn}#{content}#{rn}")
        when 'quit', 'q', 'exit'
          raise EOFError
        when 'stop'
          # TODO gracefully stop server
        when 'reload'
          puts 'reloading ...'
          load 'beanstalkd.rb'
        when 'pry'
          binding.pry
        when 'watch'
          tube_name = socket.gets(rn).chomp(rn)
          tube = find_or_create_tube(tube_name)
          # TODO update counters
          client.watch(tube)
          socket.write("WATCHING #{client.watching.size}" + rn)
        when 'ignore'
          tube_name = client.socket.gets(rn).chomp(rn)
          tube = find_tube(tube_name)
          # TODO update counters
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
              job.tube.name, # TODO check beforehand
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
              job.time_left,
              # file is the number of the earliest binlog file containing this job. If -b wasn't used, this will be 0.
              0, # TODO wal
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
        when 'reserve', 'reserve-with-timeout'
          # TODO deadline soon
          # block until found
          # TODO stats ?
          # TODO paused tube

          # reserve-with-timeout <seconds>\r\n

          # timeout :: either :eternity or 0 or number or :bad_format
          timeout = if cmd == 'receive'
                      :eternity
                    else
                      i = client.socket.readline.chomp(rn)

                      begin
                        Integer(i)
                      rescue ArgumentError
                        :bad_format
                      end
                    end

          if timeout == :bad_format
            client.socket.write(BAD_FORMAT)
            next
          end

          @consumers << client

          # reserve_timer :: either nil or timer
          reserve_timer = if Numeric === timeout && timeout != 0
                            after(timeout) { client.reserve_condition.signal :timeout }
                          else
                            nil
                          end

          loop do
            # job :: job | :timeout
            job = find_job_for(client)

            break if job

            # reserve-with-timeout
            if timeout == 0
              job = :timeout
              break
            end

            job = client.reserve_condition.wait

            # reserve-with-timeout
            break if job == :timeout
          end

          reserve_timer.cancel if reserve_timer

          @consumers.delete client

          if job == :timeout or job.nil?
            client.socket.write("TIMED_OUT\r\n")
            next
          end

          # reserve job for client
          job.owner = client
          client.jobs << job
          job.reserved!

          client.socket.write("RESERVED #{job.id} #{job.value.bytesize}#{rn}#{job.value}#{rn}")

        when 'delete'
          id = socket.readline.chomp(rn).to_i
          job = @jobs.delete(id)
          if job && ((job.owner == client && job.reserved?) || job.delayed? || job.buried?)
            job.delete

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
      on_disconnect client
    end

    def on_disconnect(client)
      # TODO on client's disconnect state call Server#on_disconnect(client) ?
      remove_client client
      @consumers.delete client
      client.transition :disconnected
    end

    def find_job_for(client)
      @jobs.values.select {|j| j.ready? && client.watching.include?(j.tube)}.min_by {|j| j.distance}
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
    BAD_FORMAT = "BAD_FORMAT\r\n".freeze

    def job_state_change(prev, current, job)
      # NOTE: be careful with symbols typos are possible
      # could change to something like Job::States::READY, Job::READY, Job::DELAYED, ..., Job.ready
      case [prev, current]
      when [:default, :ready], [:delayed, :ready]
        notify_consumer(job)
      when [:ready, :reserved]
        # continue
      when [:reserved, :ready]
        notify_consumer(job)
        job.owner.remove_job(job) #if job.owner
        job.owner = nil
      end
    end
    def notify_consumer(job)
      consumer = @consumers.find {|c| c.watching.include?(job.tube)}
      consumer && consumer.reserve_condition.signal
    end

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
#
        # job.id
        # job.priority
        # job.delay
        # job.ttr
        # job.state
        # job.value
        # job.created_at
        # job.deadline_at

        # job.tube

        # # reserves is the number of times this job has been reserved.
        # job.reserves_count
        # # timeouts is the number of times this job has timed out during a reservation.
        # job.timeouts_count
        # # releases is the number of times a client has released this job from a reservation.
        # job.releases_count
        # # buries is the number of times this job has been buried.
        # job.buries_count
        # # kicks is the number of times this job has been kicked.
        # job.kicks_count

        # job.delay_timer
        # job.ttr_timer

        # job.owner
#
        # job.owner.socket
        # job.owner.current_tube
        # job.owner.watching
        # job.owner.reserve_condition
        # job.owner.jobs
#
        # job.tube.name
        # job.tube.jobs

        # job.tube.urgent_count
        # job.tube.waiting_count
        # job.tube.buried_count
        # job.tube.reserved_count
        # job.tube.pause_count
        # job.tube.total_delete_count
        # job.tube.total_jobs_count
#
        # @default_tube = Tube.new('default')
        # @jid_seq = 0
        # @jobs = {}
        # @tubes = {'default' => @default_tube}
        # @clients = []
        # @consumers = []
