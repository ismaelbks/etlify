require "rails/generators"

module Etlify
  module Generators
    class InstallGenerator < Rails::Generators::Base
      source_root File.expand_path("templates", __dir__)

      desc "Creates the crm_sync.rb initializer"

      def create_initializer
        template "initializer.rb", "config/initializers/etlify.rb"
      end
    end
  end
end
