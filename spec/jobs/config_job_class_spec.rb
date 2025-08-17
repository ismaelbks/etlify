require "rails_helper"

RSpec.describe "Etlify sync_job_class config" do
  context "when given sync_job_class is a class" do
    before do
      class DummyJob < Etlify::SyncJob; end
      Etlify.configure { |c| c.sync_job_class = DummyJob }
    end

    let(:user) do
      User.create!(full_name: "Test User", email: "test101@example.com")
    end

    it "uses the configured job class to enqueue" do
      expect(DummyJob).to receive(:perform_later).with("User", user.id)

      user.crm_sync!(async: true)
    end
  end

  context "when given sync_job_class is a string" do
    before do
      class DummyJob < Etlify::SyncJob; end
      Etlify.configure { |c| c.sync_job_class = "DummyJob" }
    end

    let(:user) do
      User.create!(full_name: "Test User", email: "test101@example.com")
    end

    it "uses the configured job class to enqueue" do
      expect(DummyJob).to receive(:perform_later).with("User", user.id)

      user.crm_sync!(async: true)
    end
  end
end
