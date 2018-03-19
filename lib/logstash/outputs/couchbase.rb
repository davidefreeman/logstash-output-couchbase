# Output to Couchbase
# :hostname => "localhost" # :port => 8091
# :password => "secret"     # :username => 'protected'
# :bucket => "default" # :pool => "default"
# :timeout => 3_000_000
#



require "logstash/outputs/base"
require "logstash/namespace"
require 'couchbase'

class LogStash::Outputs::Couchbase < LogStash::Outputs::Base

  config_name "couchbase"
  milestone 1

  config :hostname, :validate => :string, :required => true
  config :port, :validate => :number, :required => true
  config :pool, :validate => :string, :required => true
  config :bucket, :validate => :string, :required => true
  config :username, :validate => :string, :required => true
  config :password, :validate => :string, :required => true  
  config :isodate, :validate => :boolean, :default => false
  config :ttl, :validate => :number, :default => 3000, :required => false

  public
  def register
  	require 'msgpack'
	  require 'json'

	# Connection
	Couchbase.connection_options = {
		:hosts => hostname,
		:port => port,
		:pool => pool,
                :username => username,
                :password => password,						
                :bucket => bucket
	}
	#{:async => true}
	@bucket = Couchbase.cluster.open_bucket(bucket, password)

  end # def register
  

  public
  def receive(event)
    return unless output?(event)

    begin
      document = {}.merge(event.to_hash)
      if !@isodate
        document["@timestamp"] = event.timestamp.to_s
      end
	
	#Insert
	@bucket.set(event.timestamp.to_s, document)
	  
    rescue => e
      @logger.warn("Failed to send event to Couchbase", :event => event, :exception => e,
                   :backtrace => e.backtrace)
      end
  end # def receive
end # class LogStash::Outputs::Couchbase
