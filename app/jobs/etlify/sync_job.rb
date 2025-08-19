module Etlify
  class SyncJob < ActiveJob::Base
    queue_as Etlify.config.job_queue_name
    retry_on(StandardError, attempts: 3, wait: :polynomially_longer)

    ENQUEUE_LOCK_TTL = 15.minutes
    around_enqueue do |job, block|
      key = enqueue_lock_key(job)
      locked = Etlify.config.cache_store.write(
        key,
        1,
        expires_in: ENQUEUE_LOCK_TTL,
        unless_exist: true
      )
      block.call if locked
    end

    around_perform do |job, block|
      begin
        block.call
      ensure
        Etlify.config.cache_store.delete(enqueue_lock_key(job))
      end
    end

    def perform(record_class, id)
      model  = record_class.constantize
      record = model.find_by(id: id)
      return unless record

      Etlify::Synchronizer.call(record)
    end

    private

    def enqueue_lock_key(job)
      klass, id = job.arguments
      "etlify:jobs:sync:#{klass}:#{id}"
    end
  end
end
