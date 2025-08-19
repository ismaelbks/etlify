require "rspec"
require "timecop"
require "rails/generators"

RSpec.configure do |config|
  config.order = :random
  Kernel.srand config.seed
end
