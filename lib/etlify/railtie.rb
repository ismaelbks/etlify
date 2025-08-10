require "rails/railtie"

module Etlify
  class Railtie < ::Rails::Railtie
    initializer "etlify.active_job" do
    end
  end
end
