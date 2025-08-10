require "rails_helper"
require "active_job/test_helper"
require "active_support/cache"

RSpec.describe Etlify::SyncJob do
  include ActiveJob::TestHelper

  before do
    ActiveJob::Base.queue_adapter = :test
    clear_enqueued_jobs
    clear_performed_jobs

    Etlify.config.cache_store   = ActiveSupport::Cache::MemoryStore.new
    Etlify.config.job_queue_name = "low"

    stub_const("User", Class.new do
      def self.name = "User"
      def self.to_s = name
      def self.find_by(id:) = (id == 1 ? Object.new : nil)
    end)
  end

  let(:lock_key) { "etlify:jobs:sync:User:1" }

  describe "enqueue deduplication via cache lock" do
    it(
      "enqueues only once while the lock exists, then allows again after perform",
      :aggregate_failures
    ) do
      expect { described_class.perform_later("User", 1) }
        .to change { enqueued_jobs.size }.by(1)
      expect(Etlify.config.cache_store.read(lock_key)).to eq(1)
      expect { described_class.perform_later("User", 1) }
        .not_to change { enqueued_jobs.size }
      allow(Etlify::Synchronizer).to receive(:call).and_return(:synced)
      described_class.perform_now("User", 1)
      expect(Etlify.config.cache_store.read(lock_key)).to be_nil
      expect { described_class.perform_later("User", 1) }
        .to change { enqueued_jobs.size }.by(1)
    end
  end

  describe "queue name" do
    it "uses the configured queue name" do
      expect(described_class.new.queue_name).to eq("low")
    end
  end

  describe "#perform" do
    it "calls the synchronizer when the record exists" do
      expect(Etlify::Synchronizer).to(
        receive(:call).with(instance_of(Object)).and_return(:synced)
      )
      described_class.perform_now("User", 1)
    end

    it "does nothing when the record does not exist" do
      expect(Etlify::Synchronizer).not_to receive(:call)
      described_class.perform_now("User", -1)
    end
  end

  describe "lock TTL safety" do
    it "sets a finite TTL on the enqueue lock to avoid stale keys" do
      described_class.perform_later("User", 1)
      expect(Etlify.config.cache_store.read(lock_key)).to eq(1)
    end
  end
end
