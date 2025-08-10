module Etlify
  class Config
    attr_accessor(
      :crm_adapter,
      :digest_strategy,
      :logger,
      :job_queue_name
    )

    def initialize
      @crm_adapter = Etlify::Adapters::NullAdapter.new
      @digest_strategy = Etlify::Digest.method(:stable_sha256)
      @job_queue_name = "low"
      @logger = defined?(Rails) ? Rails.logger : Logger.new($stdout)
    end
  end
end
