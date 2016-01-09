require "synapse/service_watcher/base"
require 'docker'

$docker2_servers = {}

class Docker2Server
  include Synapse::Logging
  
  attr_reader :name
  attr_reader :service_host
  
  def initialize(name, url, service_host, event_whitelist = ['start', 'die'])  
    @name = name
    @url = url
    @service_host = service_host
    @event_whitelist = event_whitelist
    @queues = []
    @cnts = []
    @cnts_lock = Mutex.new
    @watcher = Thread.new {
      run
    }
  end
  
  def cnts
    @cnts_lock.synchronize {
      @cnts
    }
  end
  
  def register
    queue = Queue.new
    @queues << queue
    return queue
  end
  
  def run
    until @should_exit
      get_containers
      send(true)
      begin
        Docker.url = @url
        Docker::Event.stream { |event|
          Thread.new {
            if @event_whitelist.include? event.status
              get_containers
              send(event)
            end
          }
        }
      rescue Docker::Error::TimeoutError => e
        next
      rescue Exception => e
        raise e
      end
    end
  end

  def send(msg)
    @queues.map do |queue|
      queue << msg
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

  def get_containers
    @cnts_lock.synchronize {
      begin
        Docker.url = @url
        @cnts = Docker::Util.parse_json(Docker.connection.get('/containers/json', {}))
        
        @cnts.each do |cnt|
          cnt['Ports'] = rewrite_container_ports(cnt['Ports'])
        end
      rescue => e
        log.warn "synapse: error polling docker host #{@url}: #{e.inspect}"
        @cnts = []
      end
    }
  end
end

class Synapse::ServiceWatcher
  class Docker2Watcher < BaseWatcher
    def start
      @wait_interval = @discovery['wait_interval'] || 0 # ms
      @event_lock = Mutex.new
      @servers = []
      
      @discovery['servers'].map do |server|
        # determine url
        if server['protocol'] == 'unix'
          url = "unix://#{server['host']}"
          service_host = '127.0.0.1'
        else
          url = "#{server['protocol'] || 'http'}://#{server['host']}:#{server['port'] || 4243}"
          service_host = server['host']
        end
        
        # create new server if it doesn't exists
        if not $docker2_servers.has_key?(url)
          $docker2_servers[url] = Docker2Server.new(server['name'], url, service_host)
          log.info "synapse: #{$docker2_servers.length} docker2 servers running."
        end
        
        @servers << url
        
        Thread.new {
          queue = $docker2_servers[url].register
          until @should_exit
            event = queue.pop
            Thread.new {
              if @event_lock.try_lock
                sleep(@wait_interval)
                set_backends(containers)
                @event_lock.unlock
              end
            }
          end
        }
      end
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

    def containers
      backends = @servers.map do |url|
        cnts = $docker2_servers[url].cnts
        # Discover containers that match the image/port we're interested in
        cnts = cnts.find_all do |cnt|
          cnt["Image"].partition(":").first == @discovery["image_name"] \
            and cnt["Ports"].has_key?(@discovery["container_port"].to_s())
        end
        cnts.map do |cnt|
          {
            'name' => $docker2_servers[url].name,
            'host' => $docker2_servers[url].service_host,
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
