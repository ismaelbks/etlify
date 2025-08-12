Etlify.configure do |config|
  # CRM adapter (must respond to #upsert!(payload:) and #delete!(crm_id:))
  # Check in lib/etlify/adapters for available adapters (e.g. Etlify::Adapters::Hubspot)
  config.crm_adapter = MyCrmAdapter.new

  # @crm_adapter = Etlify::Adapters::NullAdapter.new
  # @digest_strategy = Etlify::Digest.method(:stable_sha256)
  # @job_queue_name = "low"
  # @logger = defined?(Rails) ? Rails.logger : Logger.new($stdout)
  # @cache_store = defined?(Rails) ? Rails.cache : ActiveSupport::Cache::MemoryStore.new
end
