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
        if sync_line.stale?(digest)
          crm_id = Etlify.config.crm_adapter.upsert!(
            payload: payload,
            id_property: @record.etlify_id_property,
            object_type: @record.etlify_crm_object_type
          )

          sync_line.update!(
            crm_id: crm_id.presence,
            last_digest: digest,
            last_synced_at: Time.current,
            last_error: nil
          )

          :synced
        else
          sync_line.update!(last_synced_at: Time.current)
          :not_modified
        end
      end
    rescue StandardError => e
      sync_line.update!(last_error: e.message)
    end

    private

    def digest
      Etlify.config.digest_strategy.call(payload)
    end

    def payload
      @__payload ||= @record.build_crm_payload
    end

    def sync_line
      @record.crm_synchronisation || @record.build_crm_synchronisation
    end
  end
end
