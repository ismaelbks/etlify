require "active_support"
require "active_support/core_ext/module"
require "active_support/concern"
require "active_job"
require "logger"

require_relative "etlify/version"
require_relative "etlify/config"
require_relative "etlify/errors"
require_relative "etlify/digest"
require_relative "etlify/model"
require_relative "etlify/synchronizer"
require_relative "etlify/deleter"
require_relative "etlify/adapters/null_adapter"
require_relative "etlify/serializers/base_serializer"

require_relative "etlify/railtie" if defined?(Rails)
require_relative "etlify/engine"  if defined?(Rails)

module Etlify
  class << self

    def config
      @configuration ||= Etlify::Config.new
    end

    def configure
      yield(configuration)
    end
  end
end
