require "rails_helper"

RSpec.describe Etlify::Generators::MigrationGenerator, type: :generator do
  it "exposes a migration template" do
    path = File.expand_path(
      "../../lib/generators/etlify/migration/templates/create_crm_synchronisations.rb.tt",
      __dir__
    )
    expect(File).to exist(path)
  end
end
