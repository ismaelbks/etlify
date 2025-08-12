require "rails_helper"

RSpec.describe Etlify::Generators::InstallGenerator, type: :generator do
  it "defines an existing initializer template" do
    path = File.expand_path(
      "../../lib/generators/etlify/install/templates/initializer.rb",
      __dir__
    )
    expect(File).to exist(path)
  end
end
