require_relative "stale_records_fetcher"

module Etlify
  module BatchSync
    class StaleRecordsSyncer
      # Public API
      # Example:
      #   Etlify::BatchSync::StaleRecordsSyncer.call(since: 3.hours.ago) # async (default)
      #   Etlify::BatchSync::StaleRecordsSyncer.call(
      #     since: 1.day.ago,
      #     async: false
      #   ) # sync (for small runs or debugging)
      #
      # Options:
      # - since:       Time or anything responding to #to_time
      # - async:       true to enqueue ActiveJob, false to run in-process via Synchronizer
      # - batch_size:  number of records fetched per batch (default: 500)
      # - job_options: hash passed to ActiveJob#set
      #                 (e.g., queue: "etlify", priority: 10)
      def self.call(since:, async: true, batch_size: 500, job_options: {})
        unless since.respond_to?(:to_time)
          raise ArgumentError, "since must respond to #to_time"
        end

        stale_records = Etlify::BatchSync::StaleRecordsFetcher.updated_since(
          since.to_time
        )

        stale_records.each do |pair|
          model   = pair[:model]
          records = pair[:records]
          next if records.blank?

          primary_key = model.primary_key
          primary_key_column = [
            model.quoted_table_name,
            model.connection.quote_column_name(primary_key)
          ].join(".")
          scoped = records.reselect(primary_key_column)

          scoped.in_batches(of: batch_size) do |batch_scope|
            ids = batch_scope.pluck(primary_key)

            if async
              enqueue_jobs(ids, model, job_options)
            else
              run_sync(model, primary_key, ids)
            end
          end
        end
      end

      def self.enqueue_jobs(ids, model, job_options)
        ids.each do |id|
          Etlify.config.sync_job_class.constantize.set(**job_options).perform_later(model.name, id)
        end
      end
      private_class_method :enqueue_jobs

      def self.run_sync(model, pk, ids)
        model.where(pk => ids).find_each do |record|
          Etlify::Synchronizer.call(record)
        end
      end
      private_class_method :run_sync
    end
  end
end
