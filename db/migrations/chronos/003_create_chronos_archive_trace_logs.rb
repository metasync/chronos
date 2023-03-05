# frozen-string-literal: true

Sequel.migration do
  up do
    create_table :chronos_trace_logs do
      String :id, primary_key: true
      String :job_id, null: false
      String :target, null: false
      String :type, null: false
      DateTime :synced_at, null: false
      DateTime :traced_at, null: false
      DateTime :created_at, null: false

      index [:job_id, :created_at]
      index [:target, :job_id, :synced_at]
      index [:created_at]
    end
  end

  down do
    drop_table :chronos_trace_logs
  end
end
