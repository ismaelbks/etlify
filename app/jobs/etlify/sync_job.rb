module Etlify
  class SyncJob < ActiveJob::Base
    queue_as Etlify.config.job_queue_name
    retry_on(StandardError, attempts: 3, wait: :polynomially_longer)

    def perform(record_class, id)
      model = record_class.constantize
      record = model.find_by(id: id)
      return unless record

      Etlify::Synchronizer.call(record)
    end
  end
end
