require "rails_helper"

RSpec.describe Etlify::Generators::SerializerGenerator, type: :generator do
  it "exposes a serializer template" do
    path = File.expand_path(
      "../../lib/generators/etlify/serializer/templates/serializer.rb.tt",
      __dir__
    )
    expect(File).to exist(path)
  end
end
