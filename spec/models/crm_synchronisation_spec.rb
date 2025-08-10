require "rails_helper"

RSpec.describe CrmSynchronisation, type: :model do
  it "is stale when the digest differs", :aggregate_failures do
    line = described_class.new(last_digest: "abc")
    expect(line.stale?("xyz")).to be true
    expect(line.stale?("abc")).to be false
  end
end
