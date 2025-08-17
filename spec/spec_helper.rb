# frozen_string_literal: true

require "rspec"
require "timecop"
require "simplecov"

RSpec.configure do |config|
  config.order = :random
  Kernel.srand config.seed

  SimpleCov.start
end
