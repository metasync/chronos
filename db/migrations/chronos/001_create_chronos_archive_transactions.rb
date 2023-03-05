# frozen-string-literal: true

Sequel.migration do
  up do
    create_table :chronos_uuid_archive_transactions do
      String :id, primary_key: true
      String :archive_job_id, null: false
      String :target_uuid
      DateTime :traced_at, null: false
      DateTime :opened_at, null: false
      DateTime :closed_at

      index [:target_uuid, :archive_job_id], unique: true, name: "chronos_atxs_tuuid_ajid"
      index [:archive_job_id, :traced_at]
      index [:archive_job_id, :closed_at]
    end

    create_table :chronos_archive_transactions do
      String :id, primary_key: true
      String :archive_job_id, null: false
      Bignum :target_id
      DateTime :traced_at, null: false
      DateTime :opened_at, null: false
      DateTime :closed_at

      index [:target_id, :archive_job_id], unique: true, name: "chronos_atxs_tid_ajid"
      index [:archive_job_id, :traced_at]
      index [:archive_job_id, :closed_at]
    end
  end

  down do
    drop_table :chronos_uuid_archive_transactions
    drop_table :chronos_archive_transactions
  end
end
