require "rspec"
require "timecop"

RSpec.configure do |config|
  config.order = :random
  Kernel.srand config.seed
end
