module Etlify
  class Synchronizer
    # main entry point
    # @param record [ActiveRecord::Base]
    def self.call(record)
      new(record).call
    end

    def initialize(record)
      @record = record
    end

    def call
      @record.with_lock do
        return :not_modified unless sync_line.stale?(digest)

        crm_id = Etlify.config.crm_adapter.upsert!(payload: payload)

        sync_line.update!(
          crm_id: crm_id.presence,
          last_digest: digest,
          last_synced_at: Time.current
        )

        :synced
      end
    rescue StandardError => e
      raise Etlify::Errors::SyncError, e.message
    end

    private

    def digest
      Etlify.config.digest_strategy.call(payload)
    end

    def payload
      @record.build_crm_payload
    end

    def sync_line
      @record.crm_synchronisation || @record.build_crm_synchronisation
    end
  end
end
