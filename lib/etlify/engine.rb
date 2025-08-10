require "rails/engine"

module Etlify
  class Engine < ::Rails::Engine
    isolate_namespace Etlify
  end
end
