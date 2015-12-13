require "synapse/service_watcher/base"
require 'docker'

class Synapse::ServiceWatcher
  class Docker2Watcher < BaseWatcher
    def start
      @queue = Mutex.new
      @wait_interval = @discovery['wait_interval'] || 0 # ms
      @events = ['start', 'die']
      @latest_time = -1
      @watcher = Thread.new do
        until @should_exit
          begin
            set_backends(containers)
            Docker::Event.stream {
              |event|
              Thread.new do
                event_handle(event)
              end
            }
          rescue Docker::Error::TimeoutError => e
            next
          rescue Exception => e
            raise e
          end
        end
      end
      log.info "synapse: docker2 watcher exited successfully"
    end

    private
    def validate_discovery_opts
      raise ArgumentError, "invalid discovery method #{@discovery['method']}" \
        unless @discovery['method'] == 'docker2'
      raise ArgumentError, "a non-empty list of servers is required" \
        if @discovery['servers'].nil? or @discovery['servers'].empty?
      raise ArgumentError, "non-empty image_name required" \
        if @discovery['image_name'].nil? or @discovery['image_name'].empty?
      raise ArgumentError, "container_port required" \
        if @discovery['container_port'].nil?

      @discovery['servers'].map do |server|
          raise ArgumentError, "server host required" \
            if server['host'].nil?
      end
    end

    def event_handle(event)
      begin
        # exit if event is not in @events
        unless @events.include? event.status
          return
        end

        @queue.lock # wait for lock

        # check if event has already been handled
        if event.time <= @latest_time
          @queue.unlock
          return
        end

        sleep_until_next_check(event.time) # wait for @wait_interval seconds

        set_backends(containers) # call docker
        @queue.unlock # unlock queue
      rescue Exception => e
        log.warn "synapse: error in watcher thread: #{e.inspect}"
        log.warn e.backtrace
      end
    end

    def sleep_until_next_check(start_time)
      sleep_time = @wait_interval - (Time.now.to_i - start_time)
      if sleep_time > 0.0
        sleep(sleep_time)
      end
    end

    def rewrite_container_ports(ports)
      pairs = []
      if ports.is_a?(String)
        # "Ports" comes through (as of 0.6.5) as a string like "0.0.0.0:49153->6379/tcp, 0.0.0.0:49153->6379/tcp"
        # Convert string to a map of container port to host port: {"7000"->"49158", "6379": "49159"}
        pairs = ports.split(", ").collect do |v|
          pair = v.split('->')
          [ pair[1].rpartition("/").first, pair[0].rpartition(":").last ]
        end
      elsif ports.is_a?(Array)
        # New style API, ports is an array of hashes, with numeric values (or nil if no ports forwarded)
        pairs = ports.collect do |v|
          [v['PrivatePort'].to_s, v['PublicPort'].to_s]
        end
      end
      Hash[pairs]
    end

    def containers
      backends = @discovery['servers'].map do |server|
        server['protocol'] = server['protocol'] || 'http'
        if server['protocol'] == 'unix'
          Docker.url = "unix://#{server['host']}"
          service_host = '127.0.0.1'
        else
          Docker.url = "#{server['protocol']}://#{server['host']}:#{server['port'] || 4243}"
          service_host = server['host']
        end
        begin
          cnts = Docker::Util.parse_json(Docker.connection.get('/containers/json', {}))
          @latest_time = Time.now.to_i
        rescue => e
          log.warn "synapse: error polling docker host #{Docker.url}: #{e.inspect}"
          next []
        end
        cnts.each do |cnt|
          cnt['Ports'] = rewrite_container_ports cnt['Ports']
        end
        # Discover containers that match the image/port we're interested in
        cnts = cnts.find_all do |cnt|
          cnt["Image"].partition(":").first == @discovery["image_name"] \
            and cnt["Ports"].has_key?(@discovery["container_port"].to_s())
        end
        cnts.map do |cnt|
          {
            'name' => server['name'],
            'host' => service_host,
            'port' => cnt["Ports"][@discovery["container_port"].to_s()]
          }
        end
      end
      backends.flatten
    rescue => e
      log.warn "synapse: error while polling for containers: #{e.inspect}"
      []
    end
  end
end
