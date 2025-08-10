module Etlify
  class Deleter
    def self.call(record)
      new(record).call
    end

    def initialize(record)
      @record = record
    end

    def call
      sync_line = @record.crm_synchronisation
      return :noop unless sync_line&.crm_id.present?

      Etlify.config.crm_adapter.delete!(crm_id: sync_line.crm_id)
      :deleted
    rescue StandardError => e
      raise Etlify::Errors::SyncError, e.message
    end
  end
end
