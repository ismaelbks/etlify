module Etlify
  class Config
    attr_accessor :crm_adapter, :digest_strategy, :logger

    def initialize
      @crm_adapter = Etlify::Adapters::NullAdapter.new
      @digest_strategy = Etlify::Digest.method(:stable_sha256)
      @logger = defined?(Rails) ? Rails.logger : Logger.new($stdout)
    end
  end
end
