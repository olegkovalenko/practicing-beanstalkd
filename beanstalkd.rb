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

    # TODO constatize sybols READY = :ready, ...

    def illegal_transition(from, to)
      raise 'Illegal transition from %s to %s' % [from, to]
    end

    def start_delay_timer
      @delay_timer = actor.after(delay) do
        ready!
      end
    end

    def cancel_ttr_timer; @ttr_timer.cancel end
    def cancel_delay_timer; @delay_timer.cancel end

    def delayed!
      case @state
      when :default
        start_delay_timer
      when :delayed
        illegal_transition :delayed, :delayed
      when :ready
        illegal_transition :ready, :delayed
      when :reserved
        cancel_ttr_timer
        start_delay_timer
        @deadline_at = Time.now + delay
        @releases_count += 1
        # TODO notify tube job.tube
        actor.async.job_state_change(:reserved, :delayed, self)

      when :buried
        illegal_transition :buried, :delayed
      when :invalid then
        illegal_transition :invalid, :delayed
      end

      @state = :delayed
    end

    def ready!
      case state
      when :default
        actor.async.job_state_change(:default, :ready, self)

      when :delayed
        cancel_delay_timer
        actor.async.job_state_change(:delayed, :ready, self)

      when :ready
        illegal_transition :ready, :ready

      when :reserved
        cancel_ttr_timer
        @deadline_at = nil

        @timeouts_count += 1

        # TODO client disconnect or ttr timeout
        actor.async.job_state_change(:reserved, :ready, self)

      when :buried
        # note: kicks count updated from kick cmd # @kicks_count += 1
        actor.async.job_state_change(:buried, :ready, self)

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

        actor.async.job_state_change(:ready, :reserved, self)
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

    def buried!
      case @state
      when :default
        illegal_transition :delayed, :buried
      when :delayed
        illegal_transition :delayed, :buried
      when :ready
        illegal_transition :ready, :buried
      when :reserved
        cancel_ttr_timer
        @deadline_at = nil

        @buries_count += 1

        actor.async.job_state_change :reserved, :buried, self
      when :buried
        illegal_transition :buried, :buried
      when :invalid then
        illegal_transition :invalid, :buried
      end

      @state = :buried
    end


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

    URGENT_THRESHOLD = 1024
    def urgent?; priority < URGENT_THRESHOLD end

    def delayed?; state == :delayed end
    def reserved?; state == :reserved end
    def ready?; state == :ready end
    def buried?; state == :buried end

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
        cancel_delay_timer
      when :ready
      when :reserved
        cancel_ttr_timer
        owner.remove_job self
      when :buried
      end
      @deadline_at = nil

      tube.remove_job self

      @state = :invalid

      # TODO job.tube.total_jobs_count
    end

    def kick!
      @kicks_count += 1
      ready!
    end

    def touch!
      raise 'Job has to in reserved state' unless reserved?

      @ttr_timer.cancel
      @ttr_timer.reset
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

    attr_accessor :delay_timer,
                  :delay,
                  :state

    attr_reader :actor

    include Forwardable

    def initialize(name, jobs = [])
      @name = name
      @jobs = jobs
      @watchers = []
      @users = []

      @state = :enabled
      @delay_timer = nil
      @delay = 0

      @urgent_count = 0
      @waiting_count = 0
      @buried_count = 0
      @reserved_count = 0
      @pause_count = 0
      @total_delete_count = 0
      @total_jobs_count = 0

      @actor = Celluloid::Actor.current
    end

    def urgent_count; jobs.count &:urgent? end
    def ready_count; jobs.count &:ready? end
    def reserved_count; jobs.count &:reserved? end
    def delayed_count; jobs.count &:delayed? end
    def buried_count; jobs.count &:buried? end

    def using_count; @users.size end
    def users_count; @users.size end
    def watching_count; @watchers.size end
    def watchers_count; @watchers.size end

    alias_method :delete_count, :total_delete_count

    def time_left
      # return 0 if enabled?
      # @delay_timer.fires_in.to_i

      (enabled? && 0) || delay_timer.fires_in.to_i

      # if enabled?
      #   0
      # else
      #   @delay_timer.fires_in.to_i
      # end

      # if enabled? then 0 else @delay_timer.fires_in.to_i end
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
      @total_jobs_count += 1
      @jobs << job
    end
    def remove_job(job)
      @total_delete_count += 1
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

    def start_delay_timer
      @delay_timer = actor.after(delay) { enable! }
    end
    def cancel_delay_timer; @delay_timer.cancel end
    def pause!(delay)
      @delay = delay

      case @state
      when :enabled
        start_delay_timer
      when :disabled
        cancel_delay_timer
        start_delay_timer
      end

      # actor.async.tube_state_change(@state, :disabled, self) # don't need it

      @pause_count += 1

      @state = :disabled
    end
    alias_method :disable!, :pause!

    def enable!

      case @state
      when :enabled
        raise 'Tube transition from enabled to enabled'
      when :disabled
        @delay = 0
        cancel_delay_timer
        actor.async.tube_state_change(:disabled, :enabled, self)
      end

      @state = :enabled
    end

    def enabled?; @state == :enabled end
    # def disabled?; !enabled? end
    def disabled?; @state == :disabled end
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
        tube.remove_watcher(self)
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

    def reply_job(job)
      if job
        socket.write("FOUND #{job.id} #{job.value.bytesize}\r\n#{job.value}\r\n")
      else
        reply_not_found
      end
    end

    def reply_not_found
      socket.write(NOT_FOUND)
    end

    def ok(content)
      socket.write("OK #{content.bytesize}\r\n#{content}\r\n")
    end

    NOT_FOUND = "NOT_FOUND\r\n".freeze

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
          tube = find_or_create_tube(tube_name)
          client.use(tube)
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

            socket.write("INSERTED #{jid}" + rn)
          else
            socket.write("EXPECTED_CRLF" + rn)
          end
          # require 'pry'; binding.pry
        when 'peek'
          id = socket.readline.chomp(rn).to_i
          puts id
          job = @jobs[id]
          client.reply_job(job)
        when /peek-(ready|delayed|buried)/
          state = cmd[/ready|delayed|buried/].to_sym
          job = client.current_tube.jobs.select {|j| j.state == state}.min_by { |j| j.distance }
          client.reply_job(job)
        when 'release'
          # release <id> <pri> <delay>\r\n
          id, priority, delay = client.socket.readline(rn).chomp(rn).split(' ').map(&:to_i)
          job = @jobs[id]
          if job.owner != client
            client.reply_not_found
          else
            client.remove_job(job)

            job.owner = nil
            job.priority = priority
            job.delay = delay

            job.start

            client.socket.write "RELEASED\r\n"
          end
        when 'list-tubes-watched'
          content = client.watching.map(&:name).to_yaml
          client.ok(content)
        when 'list-tubes'
          content = @tubes.keys.to_yaml
          client.ok(content)
        when 'list-tube-used'
          client.socket.write "USING #{client.current_tube.name}\r\n"
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
            client.ok stats_content
          else
            client.reply_not_found
          end
        when 'stats-tube'
          tube_name = client.socket.gets(rn).chomp(rn)
          tube = find_tube(tube_name)
          if tube
            stats_content = STATS_TUBE_FMT % [
              tube.name, # "name: %s\n" \
              tube.urgent_count, #"current-jobs-urgent: %u\n" \
              tube.ready_count, #"current-jobs-ready: %u\n" \
              tube.reserved_count, #"current-jobs-reserved: %u\n" \
              tube.delayed_count, #"current-jobs-delayed: %u\n" \
              tube.buried_count, #"current-jobs-buried: %u\n" \
              tube.total_jobs_count, #"total-jobs: %u\n" \
              tube.users_count, #"current-using: %u\n" \
              tube.watchers_count, #"current-watching: %u\n" \
              @consumers.count {|c| c.watching.include?(tube)}, #"current-waiting: %u\n" \ tube_booking_count(tube)
              tube.total_delete_count, #"cmd-delete: %u\n" \
              tube.pause_count, #"cmd-pause-tube: %u\n" \
              tube.delay, #"pause: %u\n" \
              tube.time_left #"pause-time-left: %d\n"
            ]
            client.ok stats_content
          else
            client.reply_not_found
          end
        when 'reserve', 'reserve-with-timeout'
          # block until found or timeout (if one is given)
          # TODO deadline soon
          # TODO stats ?
          # TODO paused tube

          # timeout :: either :eternity or 0 or number or :bad_format
          timeout = if cmd == 'reserve' # NOTE typo receive
                      :eternity
                    else
                      i = client.socket.readline.chomp(rn)

                      begin
                        Integer(i)
                      rescue ArgumentError
                        :bad_format
                      end
                    end
          log = -> (txt) { puts "#{Time.now} - Client##{client.object_id} - #{cmd} > #{txt}" }
          log.call "timeout = #{timeout}"

          if timeout == :bad_format
            client.socket.write(BAD_FORMAT)
            next
          end

          @consumers << client
          log.call "added client as consumer"

          # reserve_timer :: either nil or timer
          reserve_timer = if Numeric === timeout && timeout != 0
                            after(timeout) { client.reserve_condition.signal :timeout }
                          else
                            nil
                          end

          log.call "reserve_timer = #{reserve_timer}"

          log.call "before loop"
          loop do
            # job :: job | :timeout
            job = find_job_for(client)
            log.call "job = #{job}"

            break if job

            # reserve-with-timeout
            if timeout == 0
              job = :timeout
              break
            end

            job = client.reserve_condition.wait
            log.call "job = #{job} from client reserve condition wait"

            # reserve-with-timeout
            break if job == :timeout
          end
          log.call "after loop"
          log.call "after loop job = #{job}"

          reserve_timer.cancel if reserve_timer

          @consumers.delete client
          log.call "delete from consumers"

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
        when 'bury'
          id, priority = client.socket.readline(rn).chomp(rn).split(' ').map(&:to_i)
          job = @jobs[id]
          if job and job.reserved? and job.owner == client
            client.remove_job job
            job.owner = nil
            job.priority = priority
            job.buried!

            client.socket.write "BURIED\r\n"
          else
            client.reply_not_found
          end
        when 'kick'
          count = client.socket.readline(rn).chomp(rn).to_i

          tube = client.current_tube
          kick = ->(state) { tube.jobs.select{|j| j.state == state}.take(count).each {|j| j.kick!} }

          jobs = kick.call(:buried)
          jobs = kick.call(:delayed) if jobs.empty?

          client.socket.write "KICKED #{jobs.size}\r\n"
        when 'kick-job'
          id = socket.readline.chomp(rn).to_i
          job = @jobs[id]
          if job and (job.buried? or job.delayed?)
            job.kick!
            client.socket.write "KICKED\r\n"
          else
            client.reply_not_found
          end
        when 'touch'
          id = client.socket.readline.chomp(rn).to_i
          job = @jobs[id]
          if job and job.reserved? and job.owner == client
            job.touch!
            client.socket.write "TOUCHED\r\n"
          else
            client.reply_not_found
          end
        when 'pause-tube'
          # pause-tube <tube-name> <delay>\r\n
          tube_name, delay = client.socket.readline(rn).chomp(rn).split(' ')
          delay = delay.to_i
          delay = 1 if delay == 0

          tube = find_tube(tube_name)
          if tube
            tube.pause!(delay)
            client.socket.write "PAUSED\r\n"
          else
            client.reply_not_found
          end
        else
          puts "can't handle #{cmd}"
          # client.close
          # abort(cmd + ' ' + socket.readline.inspect)
        end
      end
    rescue EOFError, Errno::ECONNRESET
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
      @jobs.values.select {|j| j.ready? && j.tube.enabled? && client.watching.include?(j.tube)}.min_by {|j| j.distance}
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

    STATS_TUBE_FMT = "---\n" \
      "name: %s\n" \
      "current-jobs-urgent: %u\n" \
      "current-jobs-ready: %u\n" \
      "current-jobs-reserved: %u\n" \
      "current-jobs-delayed: %u\n" \
      "current-jobs-buried: %u\n" \
      "total-jobs: %u\n" \
      "current-using: %u\n" \
      "current-watching: %u\n" \
      "current-waiting: %u\n" \
      "cmd-delete: %u\n" \
      "cmd-pause-tube: %u\n" \
      "pause: %u\n" \
      "pause-time-left: %d\n"

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
        job.owner.remove_job(job) if job.owner
        job.owner = nil
      when [:reserved, :delayed]
      when [:reserved, :buried]
      when [:buried, :ready]
        notify_consumer(job)
      end
    end

    def notify_consumer(job)
      return if job.tube.disabled?
      consumer = @consumers.find {|c| c.watching.include?(job.tube)}
      # consumer round robin
      if consumer
        @consumers.delete consumer
        @consumers << consumer
      end
      consumer && consumer.reserve_condition.signal
    end

    def tube_state_change(prev, current, tube)
      case [prev, current]
      when [:enabled,  :disabled]
        # TODO
      when [:disabled, :enabled]
        tube.jobs.select(&:ready?).map {|j| notify_consumer j}
      when [:disabled, :disabled]
        # pause duration extended
      else
        raise 'Tube should not transition from enabled to enabled'
      end
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
