class CrmSynchronisation < ApplicationRecord
  self.table_name = "crm_synchronisations"

  belongs_to :resource, polymorphic: true

  validates :crm_id, uniqueness: true, allow_nil: true
  validates :resource_type, presence: true
  validates :resource_id, presence: true
  validates :resource_id, uniqueness: { scope: :resource_type }

  def stale?(digest)
    last_digest != digest
  end
end
