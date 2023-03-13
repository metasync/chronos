module Chronos
  module Migration
    def self.chronos_archive_transactions(primary_key_uuid: false)
      if primary_key_uuid
        qualified_table_name(:chronos_uuid_archive_transactions)
      else
        qualified_table_name(:chronos_archive_transactions)
      end
    end

    def self.chronos_archive_transaction_logs(primary_key_uuid: false)
      if primary_key_uuid
        qualified_table_name(:chronos_uuid_archive_transaction_logs)
      else
        qualified_table_name(:chronos_archive_transaction_logs)
      end
    end

    def self.chronos_trace_logs
      qualified_table_name(:chronos_trace_logs)
    end

    def self.qualified_table_name(table_name, schema: ENV["CHRONOS_SCHEMA"])
      schema.nil? ? table_name : Sequel.qualify(schema, table_name)
    end
  end
end
