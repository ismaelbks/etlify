require "rails/engine"
require "action_dispatch/railtie"

module Etlify
  class Engine < ::Rails::Engine
    isolate_namespace Etlify
  end
end
