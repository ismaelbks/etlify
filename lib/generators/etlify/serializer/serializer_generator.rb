require "rails/generators"

module Etlify
  module Generators
    class SerializerGenerator < Rails::Generators::NamedBase
      source_root File.expand_path("templates", __dir__)
      desc "Creates a skeleton serializer that inherits from Etlify::Serializers::BaseSerializer"

      def create_serializer
        template(
          "serializer.rb.tt",
          File.join(
            "app/serializers/etlify",
            class_path,
            "#{file_name}_serializer.rb"
          )
        )
      end

      private

      def serializer_class_name
        "#{class_name}Serializer"
      end
    end
  end
end
