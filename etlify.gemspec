# frozen_string_literal: true

Gem::Specification.new do |spec|
  spec.name          = "etlify"
  spec.version       = File.read(File.expand_path("lib/etlify/version.rb", __dir__)).match(/VERSION = "([^"]+)"/)[1]
  spec.authors       = ["Capsens"]
  spec.email         = ["development@capsens.eu"]
  spec.summary       = "Idempotent synchronization between your Rails models and a CRM."
  spec.description   = "Rails DSL + adapters to synchronize your ActiveRecord resources with an external CRM using digest and idempotency."
  spec.license       = "MIT"

  spec.files = Dir.glob(
    "{app,lib,spec}/**/*",
    File::FNM_DOTMATCH
  ).select { |f| File.file?(f) } + %w[README.md]
  spec.require_paths = ["lib"]

  spec.add_dependency "rails", ">= 7.0", "< 8"
  spec.add_development_dependency "rspec"
  spec.add_development_dependency "rspec-rails", "~> 6.1"
  spec.add_development_dependency "sqlite3"
  spec.add_development_dependency "rubocop"
  spec.add_development_dependency "yard"
end
