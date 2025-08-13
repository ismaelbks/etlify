require "rails_helper"

RSpec.describe Etlify::BatchSync::StaleRecordsSyncer do
  let(:now)       { Time.utc(2025, 1, 1, 12, 0, 0) }
  let(:from_time) { now - 3.hours }

  before do
    Timecop.freeze(now)
    ActiveJob::Base.queue_adapter = :test
    ActiveJob::Base.queue_adapter.enqueued_jobs.clear
    ActiveJob::Base.queue_adapter.performed_jobs.clear
  end

  after { Timecop.return }

  def enqueued_for(job_class)
    ActiveJob::Base.queue_adapter.enqueued_jobs.select { |j| j[:job] == job_class }
  end

  def enqueued_args_for(job_class)
    enqueued_for(job_class).map { |j| j[:args] }
  end

  describe ".call" do
    it "raises ArgumentError when since does not respond to #to_time" do
      expect {
        described_class.call(since: :nope)
      }.to raise_error(ArgumentError)
    end

    it "asks StaleRecordsFetcher for pairs and enqueues one job per id",
       :aggregate_failures do
      user1 = User.create!(email: "a@etlify.test", full_name: "A")
      user2 = User.create!(email: "b@etlify.test", full_name: "B")
      rel = User.where(id: [user1.id, user2.id])

      expect(Etlify::BatchSync::StaleRecordsFetcher)
        .to receive(:updated_since).with(from_time).and_return([
          { model: User, records: rel }
        ])

      described_class.call(since: from_time, async: true, batch_size: 100)

      expect(enqueued_for(Etlify::SyncJob).size).to eq(2)
      expect(enqueued_args_for(Etlify::SyncJob))
        .to match_array([["User", user1.id], ["User", user2.id]])
    end

    it "splits work by batch_size (2,2,1 for 5 ids)", :aggregate_failures do
      ids = 5.times.map { |i| User.create!(email: "u#{i}@t.test", full_name: "U#{i}").id }
      rel = User.where(id: ids)

      allow(Etlify::BatchSync::StaleRecordsFetcher)
        .to receive(:updated_since).and_return([{ model: User, records: rel }])

      described_class.call(since: from_time, async: true, batch_size: 2)

      expect(enqueued_for(Etlify::SyncJob).size).to eq(5)
      expect(enqueued_args_for(Etlify::SyncJob))
        .to match_array(ids.map { |id| ["User", id] })
    end

    it "passes job_options to perform_later via set(...)", :aggregate_failures do
      user = User.create!(email: "q@etlify.test", full_name: "Q")
      rel = User.where(id: [user.id])

      allow(Etlify::BatchSync::StaleRecordsFetcher)
        .to receive(:updated_since).and_return([{ model: User, records: rel }])

      described_class.call(
        since: from_time,
        async: true,
        job_options: { queue: "etlify" }
      )

      jobs = enqueued_for(Etlify::SyncJob)
      expect(jobs.size).to eq(1)
      expect(jobs.first[:args]).to eq(["User", user.id])
      expect(jobs.first[:queue]).to eq("etlify")
    end

    it "runs synchronously (async: false) and calls Synchronizer", :aggregate_failures do
      user1 = User.create!(email: "s1@etlify.test", full_name: "S1")
      user2 = User.create!(email: "s2@etlify.test", full_name: "S2")
      rel = User.where(id: [user1.id, user2.id])

      allow(Etlify::BatchSync::StaleRecordsFetcher)
        .to receive(:updated_since).and_return([{ model: User, records: rel }])

      expect(Etlify::Synchronizer).to receive(:call)
        .with(have_attributes(id: user1.id)).once
      expect(Etlify::Synchronizer).to receive(:call)
        .with(have_attributes(id: user2.id)).once

      described_class.call(since: from_time, async: false, batch_size: 1)

      expect(enqueued_for(Etlify::SyncJob)).to be_empty
    end

    it "skips when records relation is blank", :aggregate_failures do
      allow(Etlify::BatchSync::StaleRecordsFetcher)
        .to receive(:updated_since).and_return([{ model: User, records: User.none }])

      expect(Etlify::Synchronizer).not_to receive(:call)

      described_class.call(since: from_time)

      expect(enqueued_for(Etlify::SyncJob)).to be_empty
    end

    it "handles multiple models (User and Company)", :aggregate_failures do
      user = User.create!(email: "m@etlify.test", full_name: "M")
      company = Company.create!(name: "Capsens", domain: "capsens.eu")

      allow(Etlify::BatchSync::StaleRecordsFetcher)
        .to receive(:updated_since).and_return([
          { model: User, records: User.where(id: [user.id]) },
          { model: Company, records: Company.where(id: [company.id]) }
        ])

      described_class.call(since: from_time)

      expect(enqueued_args_for(Etlify::SyncJob))
        .to include(["User", user.id], ["Company", company.id])
    end
  end
end
